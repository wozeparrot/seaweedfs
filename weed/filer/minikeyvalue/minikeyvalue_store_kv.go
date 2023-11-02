package minikeyvalue

import (
	"context"
	"fmt"
    "encoding/base32"
)

func (store *MKVStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	_, err = store.Request(ctx, "PUT", "kv/"+base32.HexEncoding.EncodeToString([]byte(key)), value)
	if err != nil {
		return fmt.Errorf("kv put %s: %v", key, err)
	}

	return nil
}

func (store *MKVStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	data, err := store.Request(ctx, "GET", "kv/"+base32.HexEncoding.EncodeToString([]byte(key)), nil)
	if err != nil {
		return nil, fmt.Errorf("kv get %s: %v", key, err)
	}
	return data, nil
}

func (store *MKVStore) KvDelete(ctx context.Context, key []byte) (err error) {
	_, err = store.Request(ctx, "DELETE", "kv/"+base32.HexEncoding.EncodeToString([]byte(key)), nil)
	if err != nil {
		return fmt.Errorf("kv delete %s: %v", key, err)
	}
	return nil
}
