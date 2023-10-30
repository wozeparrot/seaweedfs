package minikeyvalue

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_FILE_SEPARATOR = '.'
)

func init() {
	filer.Stores = append(filer.Stores, &MKVStore{})
}

type MKVStore struct {
	server  string
	timeout time.Duration
}

func (store *MKVStore) GetName() string {
	return "minikeyvalue"
}

func (store *MKVStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	server := configuration.GetString(prefix + "server")
	if server == "" {
		server = "http://localhost:3000"
	}

	timeout := configuration.GetString(prefix + "timeout")
	if timeout == "" {
		timeout = "3s"
	}

	return store.initialize(server, timeout)
}

func (store *MKVStore) initialize(server string, timeout string) (err error) {
	glog.Infof("filer store minikeyvalue: %s", server)

	store.server = server

	to, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("parse timeout %s: %s", timeout, err)
	}
	store.timeout = to

	// check that the server is pingable
	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", server, nil)
	if err != nil {
		return fmt.Errorf("connect to minikeyvalue %s: %v", server, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect to minikeyvalue %s: %v", server, err)
	}
	defer resp.Body.Close()

	return
}

func (store *MKVStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *MKVStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *MKVStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *MKVStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	key := genKey(entry.DirAndName())

	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = weed_util.MaybeGzipData(meta)
	}

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "PUT", store.server+string(key), bytes.NewReader(meta))
	if err != nil {
		return fmt.Errorf("persisting %s: %v", entry.FullPath, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 201 {
		return fmt.Errorf("persisting %s: %v", entry.FullPath, err)
	}
	defer resp.Body.Close()

	return nil
}

func (store *MKVStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	key := genKey(entry.DirAndName())

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "UNLINK", store.server+string(key), nil)
	if err != nil {
		return fmt.Errorf("update %s : %v", entry.FullPath, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 204 {
		return fmt.Errorf("update %s : %v", entry.FullPath, err)
	}
	defer resp.Body.Close()

	return store.InsertEntry(ctx, entry)
}

func (store *MKVStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	key := genKey(fullpath.DirAndName())

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", store.server+string(key), nil)
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	if len(body) == 0 || resp.StatusCode != 200 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(body))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *MKVStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	key := genKey(fullpath.DirAndName())

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "DELETE", store.server+string(key), nil)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 204 {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	defer resp.Body.Close()

	return nil
}

func (store *MKVStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")

	glog.V(4).Infof("delete folder children %s, prefix %s", fullpath, directoryPrefix)

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", store.server+string(directoryPrefix)+"?list", nil)
	if err != nil {
		return fmt.Errorf("list %s : %v", directoryPrefix, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("list %s : %v", directoryPrefix, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("list %s : %v", directoryPrefix, err)
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return fmt.Errorf("list %s : %v", directoryPrefix, err)
	}
	keys, ok := data["keys"].([]interface{})
	if !ok {
		return fmt.Errorf("list %s : %v", directoryPrefix, err)
	}

	for _, key := range keys {
		key := []byte(key.(string))
		ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "DELETE", store.server+string(key), nil)
		if err != nil {
			return fmt.Errorf("delete %s : %v", fullpath, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 204 {
			return fmt.Errorf("delete %s : %v", fullpath, err)
		}
		defer resp.Body.Close()
	}

	return nil
}

func (store *MKVStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *MKVStore) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	directoryPrefix := genDirectoryKeyPrefix(dirPath, "")
	lastFileStart := directoryPrefix
	if startFileName != "" {
		lastFileStart = genDirectoryKeyPrefix(dirPath, startFileName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", store.server+string(lastFileStart)+"?list", nil)
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}
	keys, ok := data["keys"].([]interface{})
	if !ok {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	for _, key := range keys {
		key := []byte(key.(string))
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(key)
		if fileName == "" {
			continue
		}
		if fileName == startFileName && !includeStartFile {
			continue
		}
		limit--
		if limit < 0 {
			break
		}
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(dirPath), fileName),
		}

		// make a request to get the entry
		ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "GET", store.server+string(key), nil)
		if err != nil {
			return lastFileName, fmt.Errorf("list %s : %v", entry.FullPath, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return lastFileName, fmt.Errorf("list %s : %v", entry.FullPath, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)

		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(body)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		if !eachEntryFunc(entry) {
			break
		}
		lastFileName = fileName
	}

	return lastFileName, err
}

func genKey(dirPath, fileName string) (key []byte) {
	// base64 urlencode every section of the dirPath
	fullpath := weed_util.NewFullPath(dirPath, "")
	fullpathSplit := fullpath.Split()
	for i, section := range fullpathSplit {
		fullpathSplit[i] = base64.URLEncoding.EncodeToString([]byte(section))
	}
	// join path
	fullpath = weed_util.JoinPath(fullpathSplit...)
	key = []byte("/")
	key = append(key, []byte(fullpath)...)
	key = append(key, DIR_FILE_SEPARATOR)
	// base64 urlencode the filename
	fileName = base64.URLEncoding.EncodeToString([]byte(fileName))
	key = append(key, []byte(fileName)...)
	return key
}

func genDirectoryKeyPrefix(fullpath weed_util.FullPath, startFileName string) (keyPrefix []byte) {
	fullpathSplit := fullpath.Split()
	for i, section := range fullpathSplit {
		fullpathSplit[i] = base64.URLEncoding.EncodeToString([]byte(section))
	}
	fullpath = weed_util.JoinPath(fullpathSplit...)
	keyPrefix = []byte("/")
	keyPrefix = append(keyPrefix, []byte(fullpath)...)
	keyPrefix = append(keyPrefix, DIR_FILE_SEPARATOR)
	if len(startFileName) > 0 {
		// base64 urlencode the filename
		startFileName = base64.URLEncoding.EncodeToString([]byte(startFileName))
		keyPrefix = append(keyPrefix, []byte(startFileName)...)
	}
	return keyPrefix
}

func getNameFromKey(key []byte) string {
	sepIndex := len(key) - 1
	for sepIndex >= 0 && key[sepIndex] != DIR_FILE_SEPARATOR {
		sepIndex--
	}

	name := string(key[sepIndex+1:])
	nameBytes, err := base64.URLEncoding.DecodeString(name)
	if err != nil {
		return ""
	}
	return string(nameBytes)
}

func (store *MKVStore) Shutdown() {}
