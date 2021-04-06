package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/mylxsw/glacier/event"

	"github.com/go-redis/redis"
	"github.com/mylxsw/asteria/log"
)

type EventStore struct {
	client    *redis.Client
	register  map[string]interface{}
	listeners map[string][]interface{}
	queueName string
	logger    log.Logger
	manager   event.Manager
}

func NewEventStore(client *redis.Client, queueName string, logger log.Logger) *EventStore {
	return &EventStore{
		client:    client,
		register:  make(map[string]interface{}),
		listeners: make(map[string][]interface{}),
		queueName: queueName,
		logger:    logger,
	}
}

func (rs *EventStore) Listen(evtType string, listener interface{}) {
	if _, ok := rs.listeners[evtType]; !ok {
		rs.listeners[evtType] = make([]interface{}, 0)
	}

	rs.listeners[evtType] = append(rs.listeners[evtType], listener)
}

func (rs *EventStore) Publish(evt event.Event) {
	data, err := encode(evt.Event)
	if err != nil {
		rs.logger.With(evt).Errorf("encode event failed: %v", err)
		return
	}

	if err := rs.client.LPush(rs.queueName, fmt.Sprintf("%s,%s", evt.Name, string(data))).Err(); err != nil {
		rs.logger.With(evt).Errorf("push event data to redis failed: %v", err)
		return
	}
}

func (rs *EventStore) SetManager(manager event.Manager) {
	rs.manager = manager
}

func (rs *EventStore) Start(ctx context.Context) <-chan interface{} {
	stop := make(chan interface{}, 0)

	go func() {
		for {
			select {
			case <-ctx.Done():
				stop <- struct{}{}
			default:
				results, err := rs.client.BRPop(1*time.Second, rs.queueName).Result()
				if err != nil {
					if err != redis.Nil {
						rs.logger.Errorf("brpop from redis failed: %v", err)
					}
					continue
				}

				data := strings.SplitN(results[1], ",", 2)
				if listeners, ok := rs.listeners[data[0]]; ok {
					if typ, ok := rs.register[data[0]]; ok {
						value, err := decode([]byte(data[1]), typ)
						if err != nil {
							rs.logger.Errorf("decode serialized data from redis failed: %v", err)
							continue
						}

						var wg sync.WaitGroup
						wg.Add(len(listeners))
						for i, ls := range listeners {
							go (func(ls interface{}, i int) {
								defer func() {
									if err := recover(); err != nil {
										rs.logger.With(value).Errorf("event listener %d execute failed: %v", i, err)
									}
									wg.Done()
								}()

								rs.manager.Call(value, ls)
							})(ls, i)
						}

						wg.Wait()
					} else {
						rs.logger.Errorf("event not register: %s", data[0])
					}
				} else {
					rs.logger.Warningf("no listeners for event: %s", data[0])
				}
			}
		}
	}()
	return stop
}

func (rs *EventStore) Register(typ interface{}) {
	rs.register[fmt.Sprintf("%s", reflect.TypeOf(typ))] = typ
}

func encode(evt interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(evt); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decode(data []byte, decodeToObj interface{}) (interface{}, error) {
	value := reflect.New(reflect.TypeOf(decodeToObj))
	if err := gob.NewDecoder(bytes.NewBuffer(data)).DecodeValue(value); err != nil {
		return nil, err
	}

	return value.Elem().Interface(), nil
}