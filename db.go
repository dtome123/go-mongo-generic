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

// Option defines a function to configure dbOptions.
type Option func(*dbOptions)

type dbOptions struct {
	writeURL          string
	readURL           string
	database          string
	monitor           *event.CommandMonitor
	timeout           time.Duration
	separateReadWrite bool
}

// WithSingleURL sets a single MongoDB URL for both read and write operations.
func WithSingleURL(url string) Option {
	return func(o *dbOptions) {
		o.writeURL = url
		o.readURL = url
		o.separateReadWrite = false
	}
}

// WithMongoURLs sets separate MongoDB URLs for write and read operations.
// If readURL is empty, it defaults to writeURL.
func WithMongoURLs(writeURL, readURL string) Option {
	return func(o *dbOptions) {
		o.writeURL = writeURL
		if readURL == "" {
			o.readURL = writeURL
		} else {
			o.readURL = readURL
		}
		o.separateReadWrite = true
	}
}

// WithSeparateReadWrite enables or disables separate read/write connections.
// Only has effect if URLs are set separately.
func WithSeparateReadWrite(enabled bool) Option {
	return func(o *dbOptions) {
		o.separateReadWrite = enabled
	}
}

// WithDatabase sets the database name.
func WithDatabase(db string) Option {
	return func(o *dbOptions) {
		o.database = db
	}
}

// WithMonitor sets a command monitor for metrics/logging.
func WithMonitor(monitor *event.CommandMonitor) Option {
	return func(o *dbOptions) {
		o.monitor = monitor
	}
}

// WithTimeout sets a timeout for connection context.
func WithTimeout(timeout time.Duration) Option {
	return func(o *dbOptions) {
		o.timeout = timeout
	}
}

type Database struct {
	writeDB *mongo.Database
	readDB  *mongo.Database
}

// NewDatabase creates a new Database connection with the given options.
func NewDatabase(opts ...Option) (*Database, error) {
	options := &dbOptions{
		timeout:           10 * time.Second,
		separateReadWrite: true, // default to true
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.writeURL == "" {
		return nil, errors.New("write URL is required")
	}

	// if separateReadWrite is false, force readURL = writeURL
	if !options.separateReadWrite {
		options.readURL = options.writeURL
	} else {
		// separateReadWrite = true but readURL empty fallback to writeURL
		if options.readURL == "" {
			options.readURL = options.writeURL
		}
	}

	if options.database == "" {
		return nil, errors.New("database name is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), options.timeout)
	defer cancel()

	writeClientOpts := optionsMongoClient(options.writeURL, options.monitor, readpref.Primary())
	writeClient, err := mongo.Connect(ctx, writeClientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to write DB: %w", err)
	}

	readClientOpts := optionsMongoClient(options.readURL, options.monitor, readpref.SecondaryPreferred())
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

	return &Database{
		writeDB: writeClient.Database(options.database),
		readDB:  readClient.Database(options.database),
	}, nil
}

func optionsMongoClient(uri string, monitor *event.CommandMonitor, rp *readpref.ReadPref) *options.ClientOptions {
	opts := options.Client().ApplyURI(uri).SetReadPreference(rp)
	if monitor != nil {
		opts.SetMonitor(monitor)
	}
	return opts
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
