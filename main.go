package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

var mode = flag.String("mode", "", "")

func main() {
	flag.Parse()

	var err error

	switch *mode {
	case "load":
		err = load()
	case "changefeed":
		err = changefeed()
	case "get":
		err = get()
	case "clean":
		err = clean()
	default:
		log.Printf("--mode {load, changefeed, get, clean}")
	}

	if err != nil {
		log.Fatal(err.Error())
	}

	log.Printf("done")
}

func clean() error {
	ctx := context.Background()

	conn, err := newDBConn()
	if err != nil {
		return err
	}

	defer conn.Close()

	return cleanDB(ctx, conn)
}

func load() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)

	loaderConn, err := newDBConn()
	if err != nil {
		return err
	}

	eg.Go(func() error {
		<-ctx.Done()
		return loaderConn.Close()
	})

	eg.Go(func() error {
		defer cancel()

		loader := &Loader{db: loaderConn}
		log.Print("starting to load events")

		numEventsToLoad := 100
		for count := 0; count < numEventsToLoad; count++ {

			err := loader.Load(ctx, &types.Empty{})
			if err != nil {
				return err
			}
		}

		log.Printf("loaded %v events", numEventsToLoad)
		return nil
	})

	return eg.Wait()
}

func get() error {
	ctx := context.Background()

	dbConn, err := newDBConn()
	if err != nil {
		return err
	}

	events, err := getEvents(ctx, dbConn, accountID)
	if err != nil {
		return err
	}

	for _, v := range events {
		log.Print(v.String())
	}

	return nil
}

type Loader struct {
	db *sql.DB
}

var accountID = "979e1cb9-5b2c-415d-9bed-58df67352e82"

func (l *Loader) Load(ctx context.Context, msg proto.Message) error {
	any, err := types.MarshalAny(msg)
	if err != nil {
		return err
	}

	payload, err := proto.Marshal(any)
	if err != nil {
		return err
	}

	event := Event{
		ID:        uuid.New().String(),
		AccountID: accountID,
		// AccountID:  uuid.New().String(),
		Payload:    string(payload),
		ObservedAt: time.Now(),
	}

	return insertEvent(ctx, l.db, event)
}

func changefeed() error {
	eg, ctx := errgroup.WithContext(context.Background())

	changefeederDB, err := newDBConn()
	if err != nil {
		return err
	}
	eg.Go(func() error {
		<-ctx.Done()
		return changefeederDB.Close()
	})

	eg.Go(func() error {
		changefeeder := &changefeeder{db: changefeederDB}

		log.Print("starting changefeed")
		err := changefeeder.changefeed(ctx)
		if err != nil {
			return err
		}

		log.Print("stopped changefeed")
		return nil
	})

	return eg.Wait()
}

type changefeeder struct {
	db *sql.DB
}

func (l *changefeeder) changefeed(ctx context.Context) error {
	return readChangeFeed(ctx, l.db)
}
