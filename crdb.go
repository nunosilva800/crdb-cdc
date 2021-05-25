package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/cockroachdb"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/lib/pq"
	"github.com/nunosilva/crdb-cdc/db"
)

func newDBConn() (*sql.DB, error) {
	d, err := iofs.New(db.Migrations, "migrations")
	if err != nil {
		log.Fatal(err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", d, "cockroachdb://root@localhost:26257/crdb_test?sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("opening migrations: %w", err)
	}

	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	sqlDB, err := sql.Open("postgres", "postgres://root@localhost:26257/crdb_test?sslmode=disable")
	if err != nil {
		return nil, err
	}

	return sqlDB, nil
}

func cleanDB(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, `DELETE FROM events`)
	return err
}

func insertEvent(ctx context.Context, conn *sql.DB, event Event) error {
	_, err := conn.ExecContext(
		ctx,
		`
		INSERT INTO events (event_id, account_id, payload, observed_at)
		VALUES ($1, $2, $3, $4)
		`,
		event.ID,
		event.AccountID,
		event.Payload,
		event.ObservedAt,
	)
	return err
}

func getEvents(ctx context.Context, conn *sql.DB, account_id string) ([]Event, error) {
	rows, err := conn.QueryContext(
		ctx,
		`
		SELECT event_id, account_id, payload, observed_at
		FROM events
		WHERE account_id = $1
		ORDER BY observed_at
		`,
		account_id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Event

	for rows.Next() {
		var event Event
		err := rows.Scan(
			&event.ID,
			&event.AccountID,
			&event.Payload,
			&event.ObservedAt,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, event)
	}

	return results, nil
}

func readChangeFeed(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `EXPERIMENTAL CHANGEFEED FOR events;`)
	if err != nil {
		return err
	}

	count := 0
	for rows.Next() {
		cdc := CDC{}
		err := rows.Scan(&cdc.table, &cdc.key, &cdc.value)
		if err != nil {
			log.Printf("scanning: %v", err.Error())
		}
		count++
		if count%10 == 0 {
			log.Printf("so far got %v", count)
			log.Printf("sample: %v\n", cdc)

			val := CDCValue{}
			err := json.Unmarshal([]byte(cdc.value), &val)
			if err != nil {
				return err
			}
			log.Println(val.After.AsEvent().String())

		}
	}

	return nil
}

type Event struct {
	ID         string    `json:"event_id"`
	AccountID  string    `json:"account_id"`
	Payload    []byte    `json:"payload"`
	ObservedAt time.Time `json:"observed_at"`
}

func (e Event) String() string {
	var any types.Any
	err := proto.Unmarshal(e.Payload, &any)
	if err != nil {
		log.Print("error unmarshalling payload: ", err.Error())
		return fmt.Sprintf("%v: n/a", e.ObservedAt)
	}

	var dAny types.DynamicAny
	err = types.UnmarshalAny(&any, &dAny)
	if err != nil {
		log.Print("error unmarshalling any: ", err.Error())
		return fmt.Sprintf("%v: n/a", e.ObservedAt)
	}
	return fmt.Sprintf("%v: %v", e.ObservedAt, dAny.String())
}

type CDC struct {
	table string
	key   string
	value string
}

type CDCValue struct {
	After CDCValueEvent `json:"after"`
}

type CDCValueEvent struct {
	ID         string `json:"event_id"`
	AccountID  string `json:"account_id"`
	Payload    string `json:"payload"`
	ObservedAt string `json:"observed_at"`
}

func (e *CDCValueEvent) AsEvent() Event {
	b, err := base64.StdEncoding.DecodeString(e.Payload)
	if err != nil {
		log.Print("error decoding payload: ", err.Error())
	}

	t, err := time.Parse("2006-01-02T15:04:05.999999999", e.ObservedAt)
	if err != nil {
		log.Print("error convering observed at: ", err.Error())
	}

	event := Event{
		ID:         e.ID,
		AccountID:  e.AccountID,
		Payload:    b,
		ObservedAt: t,
	}
	return event
}
