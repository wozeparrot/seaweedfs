package minikeyvalue

import (
	"context"
	"fmt"
    "net/http"
	"bytes"
    "io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *MKVStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

    ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
    defer cancel()
    req, err := http.NewRequestWithContext(ctx, "PUT", store.server + "/kv/" + string(key), bytes.NewReader(value))
    if err != nil {
        return fmt.Errorf("kv put %s: %v", key, err)
    }
    resp, err := http.DefaultClient.Do(req)
    if err != nil || resp.StatusCode != 201 {
        return fmt.Errorf("kv put %s: %v", key, err)
    }
    defer resp.Body.Close()


	return nil
}

func (store *MKVStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

    ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
    defer cancel()
    req, err := http.NewRequestWithContext(ctx, "GET", store.server + "/kv/" + string(key), nil)
    if err != nil {
        return nil, fmt.Errorf("kv get %s : %v", key, err)
    }
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, fmt.Errorf("kv get %s : %v", key, err)
    }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, fmt.Errorf("kv get %s : %v", key, err)
    }

    if len(body) == 0 || resp.StatusCode != 200 {
        return nil, filer.ErrKvNotFound
    }

	return body, nil
}

func (store *MKVStore) KvDelete(ctx context.Context, key []byte) (err error) {

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
    defer cancel()
    req, err := http.NewRequestWithContext(ctx, "DELETE", store.server + "/kv/" + string(key), nil)
    if err != nil {
        return fmt.Errorf("kv delete %s : %v", key, err)
    }
    resp, err := http.DefaultClient.Do(req)
    if err != nil || resp.StatusCode != 204 {
        return fmt.Errorf("kv delete %s : %v", key, err)
    }
    defer resp.Body.Close()


	return nil
}
