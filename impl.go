package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type implCollection[T CollectionModel] struct {
	writeCollection *mongo.Collection
	readCollection  *mongo.Collection
	collectionName  string
}

func NewCollection[T CollectionModel](db *Database) Collection[T] {

	var model T
	collectionName := model.CollectionName()
	return &implCollection[T]{
		writeCollection: db.writeDB.Collection(collectionName),
		readCollection:  db.readDB.Collection(collectionName),
		collectionName:  collectionName,
	}

}

func (d *implCollection[T]) GetCollection() *mongo.Collection {
	return d.writeCollection
}

func (d *implCollection[T]) Find(ctx context.Context, filter interface{}, opt *options.FindOptions, pagination *Pagination) ([]*T, error) {
	if pagination != nil {
		total, err := d.readCollection.CountDocuments(ctx, filter)
		if err != nil {
			return nil, err
		}
		pagination.Init(int64(pagination.PageCurrent), int64(pagination.PageLimit))
		pagination.SetTotalRecord(total)
		opt.SetSkip(pagination.Offset)
		opt.SetLimit(pagination.PageLimit)
	}

	cur, err := d.readCollection.Find(ctx, filter, opt)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var results []*T
	if err := cur.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func (d *implCollection[T]) FindOne(ctx context.Context, filter interface{}, opt *options.FindOneOptions) (*T, error) {
	result := new(T)
	if err := d.readCollection.FindOne(ctx, filter, opt).Decode(result); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *implCollection[T]) FindOneAndUpdate(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.FindOneAndUpdateOptions,
) (*T, error) {
	result := new(T)
	err := d.writeCollection.FindOneAndUpdate(ctx, filter, update, opts...).Decode(result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *implCollection[T]) InsertOne(ctx context.Context, document T) error {
	_, err := d.writeCollection.InsertOne(ctx, document)
	return err
}

func (d *implCollection[T]) InsertMany(ctx context.Context, documents []*T) error {
	docs := make([]interface{}, len(documents))
	for i, doc := range documents {
		docs[i] = doc
	}
	_, err := d.writeCollection.InsertMany(ctx, docs)
	return err
}

func (d *implCollection[T]) UpdateOne(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) error {
	_, err := d.writeCollection.UpdateOne(ctx, filter, update, opts...)
	return err
}

func (d *implCollection[T]) UpdateSetOne(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) error {
	return d.UpdateOne(ctx, filter, bson.M{"$set": update}, opts...)
}

func (d *implCollection[T]) UpdateMany(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) error {
	_, err := d.writeCollection.UpdateMany(ctx, filter, update, opts...)
	return err
}

func (d *implCollection[T]) Delete(ctx context.Context, filter interface{}, opts *options.DeleteOptions) error {
	_, err := d.writeCollection.DeleteMany(ctx, filter, opts)
	return err
}

func (d *implCollection[T]) Count(ctx context.Context, filter interface{}, opts *options.CountOptions) (int64, error) {
	return d.readCollection.CountDocuments(ctx, filter, opts)
}

func (d *implCollection[T]) BulkWrite(
	ctx context.Context,
	operations []mongo.WriteModel,
	opts *options.BulkWriteOptions,
) (*mongo.BulkWriteResult, error) {
	return d.writeCollection.BulkWrite(ctx, operations, opts)
}

func (d *implCollection[T]) EnsureIndexes(indexes []mongo.IndexModel) error {
	ctx := context.Background()

	// Step 1: Get existing index names
	type mongoIndex struct {
		Name string `bson:"name"`
	}
	existing := make(map[string]struct{})
	cursor, err := d.writeCollection.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list indexes failed: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var idx mongoIndex
		if err := cursor.Decode(&idx); err != nil {
			return fmt.Errorf("decode index failed: %w", err)
		}
		if idx.Name != "_id_" {
			existing[idx.Name] = struct{}{}
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	// Step 2: Collect expected index names
	expected := make(map[string]struct{}, len(indexes))
	for _, idx := range indexes {
		if idx.Options != nil && idx.Options.Name != nil {
			expected[*idx.Options.Name] = struct{}{}
		}
	}

	// Step 3: Drop indexes not in expected
	for name := range existing {
		if _, ok := expected[name]; !ok {
			if _, err := d.writeCollection.Indexes().DropOne(ctx, name); err != nil {
				return fmt.Errorf("drop index %s failed: %w", name, err)
			}
		}
	}

	// Step 4: Create expected indexes
	for _, idx := range indexes {
		if _, err := d.writeCollection.Indexes().CreateOne(ctx, idx); err != nil {
			return fmt.Errorf("create index failed: %w", err)
		}
	}

	return nil
}
