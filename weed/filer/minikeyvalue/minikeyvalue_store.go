package minikeyvalue

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &MKVStore{})
    EncodedDirKeySize = base32.HexEncoding.EncodedLen(sha1.Size)
}

type MKVStore struct {
	server string
}

func (store *MKVStore) GetName() string {
	return "minikeyvalue"
}

func (store *MKVStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	store.server = configuration.GetString(prefix + "server")
	if store.server == "" {
		store.server = "http://localhost:3000/"
	}

	return nil
}

func (store *MKVStore) Shutdown() {}

func (store *MKVStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *MKVStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *MKVStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *MKVStore) Request(ctx context.Context, method string, path string, body []byte) (ret []byte, err error) {
    ctx, cancel := context.WithTimeout(ctx, weed_util.RetryWaitTime)
    defer cancel()
	req := &http.Request{}
	if body != nil {
		req, err = http.NewRequestWithContext(ctx, method, store.server+path, bytes.NewReader(body))
	} else {
		req, err = http.NewRequestWithContext(ctx, method, store.server+path, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %v", err)
	}
	defer resp.Body.Close()
	ret, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %v", err)
	}
	return ret, nil
}

func (store *MKVStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	key := genKey(entry.DirAndName())

	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode entry %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = weed_util.MaybeGzipData(meta)
	}

	_, err = store.Request(ctx, "PUT", key, meta)
	if err != nil {
		return fmt.Errorf("persisting entry %s: %v", entry.FullPath, err)
	}

	return nil
}

func (store *MKVStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	key := genKey(entry.DirAndName())

	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode entry %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = weed_util.MaybeGzipData(meta)
	}

	_, err = store.Request(ctx, "PATCH", key, meta)
	if err != nil {
		return fmt.Errorf("updating entry %s: %v", entry.FullPath, err)
	}

	return nil
}

func (store *MKVStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	key := genKey(fullpath.DirAndName())

	data, err := store.Request(ctx, "GET", key, nil)
	if err != nil {
		return nil, fmt.Errorf("finding entry %s: %v", fullpath, err)
	}

	if data == nil || len(data) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(data))
	if err != nil {
		return nil, fmt.Errorf("decoding entry %s: %v", fullpath, err)
	}

	return entry, nil
}

func (store *MKVStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	key := genKey(fullpath.DirAndName())

    data, err := store.Request(ctx, "DELETE", key, nil)
	if err != nil || data == nil {
		return fmt.Errorf("deleting entry %s: %v", fullpath, err)
	}

	return nil
}

func (store *MKVStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	directoryKey := genDirectoryKey(string(fullpath))

	data, err := store.Request(ctx, "GET", directoryKey+"?list", nil)
	if err != nil || data == nil {
		return fmt.Errorf("finding directory %s: %v", fullpath, err)
	}

	var parsedData map[string]interface{}
	err = json.Unmarshal(data, &parsedData)
	if err != nil {
		return fmt.Errorf("parsing directory %s: %v", fullpath, err)
	}
	keys, ok := parsedData["keys"].([]interface{})
	if !ok {
		return fmt.Errorf("parsing directory keys %s: %v", fullpath, err)
	}

	for _, key := range keys {
		key := key.(string)[1:]
		_, err = store.Request(ctx, "DELETE", key, nil)
		if err != nil {
			return fmt.Errorf("deleting entry %s: %v", fullpath, err)
		}
	}

	return nil
}

func (store *MKVStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *MKVStore) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	directoryKey := genDirectoryKey(string(dirPath))
	encodedStartFileName := encodeFileName(startFileName)
	lastFileStart := directoryKey + encodedStartFileName

    glog.V(4).Infof("list directory %s from %s", dirPath, lastFileStart)

	data, err := store.Request(ctx, "GET", directoryKey+"?list&limit="+fmt.Sprint(limit)+"&start=/"+lastFileStart, nil)
	if err != nil || data == nil {
		return lastFileName, fmt.Errorf("finding directory %s: %v", dirPath, err)
	}

	var parsedData map[string]interface{}
	err = json.Unmarshal(data, &parsedData)
	if err != nil {
		return lastFileName, fmt.Errorf("parsing directory %s: %v", dirPath, err)
	}
	keys, ok := parsedData["keys"].([]interface{})
	if !ok {
		return lastFileName, fmt.Errorf("parsing directory keys %s: %v", dirPath, err)
	}

	for _, key := range keys {
		key := key.(string)[1:]
		fileName := getNameFromKey(key)
		if fileName == "" || (!includeStartFile && fileName == encodedStartFileName) {
			continue
		}

		limit--
		if limit < 0 {
			break
		}

		decodedFileName, err := decodeFileName(fileName)
		if err != nil {
			glog.V(0).Infof("decoding file name %s: %v", fileName, err)
			break
		}
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(dirPath), decodedFileName),
		}

		data, err := store.Request(ctx, "GET", key, nil)
		if err != nil {
			glog.V(0).Infof("finding entry %s: %v", entry.FullPath, err)
			break
		}

		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(data)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("decoding entry %s: %v", entry.FullPath, err)
			break
		}

		if !eachEntryFunc(entry) {
			break
		}
		lastFileName = decodedFileName
	}

	return lastFileName, err
}

func hashToString(str string) string {
    s := sha1.Sum([]byte(str))
    return base32.HexEncoding.EncodeToString(s[:])
}

func encodeFileName(name string) string {
	return base32.HexEncoding.EncodeToString([]byte(name))
}

func decodeFileName(name string) (string, error) {
	data, err := base32.HexEncoding.DecodeString(name)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func genKey(dir string, name string) string {
	key := hashToString(dir)
	name = encodeFileName(name)
	return key + name
}

func genDirectoryKey(dir string) string {
	return hashToString(dir)
}

var EncodedDirKeySize int
func getNameFromKey(key string) string {
	return key[EncodedDirKeySize:]
}
