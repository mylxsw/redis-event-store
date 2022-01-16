package store_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/mylxsw/asteria/log"
	"github.com/mylxsw/glacier/event"
	"github.com/mylxsw/redis-event-store/store"
)

type SystemUpDownEvent struct {
	Name string
}

func TestEventStore(t *testing.T) {
	logger := log.Module("redis-event-store")

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	evtStore := store.NewEventStore(client, "event-store", logger)
	manager := event.NewEventManager(evtStore)
	evtStore.SetManager(manager)

	evtStore.Register(SystemUpDownEvent{})

	evt := event.Event{
		Name:  reflect.TypeOf(SystemUpDownEvent{}).String(),
		Event: SystemUpDownEvent{Name: "up"},
	}

	if err := evtStore.Publish(evt); err != nil {
		panic(err)
	}
	if err := evtStore.Publish(evt); err != nil {
		panic(err)
	}
	if err := evtStore.Publish(evt); err != nil {
		panic(err)
	}

	evtStore.Listen(evt.Name, func(ev SystemUpDownEvent) {
		logger.Debugf("new event received: %v", ev)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	<-evtStore.Start(ctx)
}
