package txstorage

import (
	"errors"

	"github.com/easy-bus/bus"
	"github.com/gomodule/redigo/redis"
	"github.com/letsfire/redigo/v2"
	"github.com/letsfire/utils"
)

type redisTxstorage struct {
	hashMap string
	client  *redigo.Client
}

func (rts *redisTxstorage) Store(data []byte) (string, error) {
	id := utils.GenerateSeqId()
	num, err := rts.client.Int(func(c redis.Conn) (interface{}, error) {
		return c.Do("HSET", rts.hashMap, id, data)
	})
	if err == nil && num == 0 {
		err = errors.New("redis tx storage store failed")
	}
	return id, err
}

func (rts *redisTxstorage) Fetch(id string) ([]byte, error) {
	res, err := rts.client.Bytes(func(c redis.Conn) (interface{}, error) {
		return c.Do("HGET", rts.hashMap, id)
	})
	if err == redis.ErrNil {
		return nil, nil
	}
	return res, err
}

func (rts *redisTxstorage) Remove(id string) error {
	_, err := rts.client.Execute(func(c redis.Conn) (interface{}, error) {
		return c.Do("HDEL", rts.hashMap, id)
	})
	return err
}

func NewRedis(hashMap string, client *redigo.Client) bus.TXStorageInterface {
	return &redisTxstorage{hashMap: hashMap, client: client}
}
