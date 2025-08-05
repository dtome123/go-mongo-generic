package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Database struct {
	writeDB *mongo.Database
	readDB  *mongo.Database
}

// New creates a new Database connection with separate write/read DB.
func New(config *Config) (*Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	writeClientOpts := options.Client().
		ApplyURI(config.WriteURL).
		SetReadPreference(readpref.Primary())

	writeClient, err := mongo.Connect(ctx, writeClientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to write DB: %w", err)
	}

	readClientOpts := options.Client().
		ApplyURI(config.ReadURL).
		SetReadPreference(readpref.SecondaryPreferred())

	readClient, err := mongo.Connect(ctx, readClientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to read DB: %w", err)
	}

	if err := writeClient.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("write DB ping failed: %w", err)
	}

	if err := readClient.Ping(ctx, readpref.SecondaryPreferred()); err != nil {
		return nil, fmt.Errorf("read DB ping failed: %w", err)
	}

	if config.Database == "" {
		return nil, errors.New("no database name configured")
	}

	return &Database{
		writeDB: writeClient.Database(config.Database),
		readDB:  readClient.Database(config.Database),
	}, nil
}

// NewWithMonitor creates a Database connection with a command monitor (for metrics/logging).
func NewWithMonitor(config *Config, monitor *event.CommandMonitor) (*Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	writeClientOpts := options.Client().
		ApplyURI(config.WriteURL).
		SetMonitor(monitor).
		SetReadPreference(readpref.Primary())

	writeClient, err := mongo.Connect(ctx, writeClientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to write DB: %w", err)
	}

	readClientOpts := options.Client().
		ApplyURI(config.ReadURL).
		SetMonitor(monitor).
		SetReadPreference(readpref.SecondaryPreferred())

	readClient, err := mongo.Connect(ctx, readClientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to read DB: %w", err)
	}

	if err := writeClient.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("write DB ping failed: %w", err)
	}

	if err := readClient.Ping(ctx, readpref.SecondaryPreferred()); err != nil {
		return nil, fmt.Errorf("read DB ping failed: %w", err)
	}

	if config.Database == "" {
		return nil, errors.New("no database name configured")
	}

	return &Database{
		writeDB: writeClient.Database(config.Database),
		readDB:  readClient.Database(config.Database),
	}, nil
}

// WithTransaction executes a callback inside a MongoDB transaction.
func (d *Database) WithTransaction(ctx context.Context, callback func(sc mongo.SessionContext) (interface{}, error)) error {
	session, err := d.writeDB.Client().StartSession()
	if err != nil {
		return fmt.Errorf("start session failed: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	return nil
}

// ReadCollection returns a read-only collection.
func (d *Database) ReadCollection(name string) *mongo.Collection {
	return d.readDB.Collection(name)
}

// WriteCollection returns a write-only collection.
func (d *Database) WriteCollection(name string) *mongo.Collection {
	return d.writeDB.Collection(name)
}

// Aggregate executes an aggregation pipeline and decodes result into []*V.
func Aggregate[V any](
	ctx context.Context,
	collection *mongo.Collection,
	pipeline mongo.Pipeline,
	opts *options.AggregateOptions,
) ([]*V, error) {
	cur, err := collection.Aggregate(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("aggregate query failed: %w", err)
	}
	defer cur.Close(ctx)

	var results []*V
	if err := cur.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode aggregation result: %w", err)
	}

	return results, nil
}
