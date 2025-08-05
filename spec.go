package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Collection[T CollectionModel] interface {
	GetCollection() *mongo.Collection

	// CRUD
	Find(ctx context.Context, filter interface{}, opts *options.FindOptions, pagination *Pagination) ([]*T, error)
	FindOne(ctx context.Context, filter interface{}, opts *options.FindOneOptions) (*T, error)
	InsertOne(ctx context.Context, document T) error
	InsertMany(ctx context.Context, documents []*T) error
	Delete(ctx context.Context, filter interface{}, opts *options.DeleteOptions) error
	Count(ctx context.Context, filter interface{}, opts *options.CountOptions) (int64, error)
	UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) error
	UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) error
	FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...*options.FindOneAndUpdateOptions) (*T, error)
	UpdateSetOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) error

	// Bulk
	BulkWrite(ctx context.Context, operations []mongo.WriteModel, opts *options.BulkWriteOptions) (*mongo.BulkWriteResult, error)

	// Index
	EnsureIndexes(indexes []mongo.IndexModel) error
}

type CollectionModel interface {
	CollectionName() string
}
