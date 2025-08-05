package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CollectionRepository[T Collection] interface {
	GetCollection() *mongo.Collection

	Find(ctx context.Context, filter interface{}, opt *options.FindOptions, pagination *Pagination) ([]*T, error)
	FindOne(ctx context.Context, filter interface{}, opt *options.FindOneOptions) (*T, error)
	FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opt *options.FindOneAndUpdateOptions) (*T, error)

	InsertOne(ctx context.Context, document T) error
	InsertMany(ctx context.Context, documents []*T) error

	UpdateOne(ctx context.Context, filter interface{}, update interface{}, options *options.UpdateOptions) error
	UpdateMany(ctx context.Context, filter interface{}, update interface{}, options *options.UpdateOptions) error

	Delete(ctx context.Context, filter interface{}, options *options.DeleteOptions) error
	Count(ctx context.Context, filter interface{}, opts *options.CountOptions) (int64, error)

	BulkWrite(ctx context.Context, operations []mongo.WriteModel, opts *options.BulkWriteOptions) (*mongo.BulkWriteResult, error)

	EnsureIndexes(indexes []mongo.IndexModel) error
}

type Collection interface {
	CollectionName() string
}
