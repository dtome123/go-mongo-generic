package mongodb

import (
	"context"
	"errors"
	"log"
	"sort"
	"time"

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

func (d *implCollection[T]) Find(ctx context.Context, filter interface{}, opt *options.FindOptions, pagination *Pagination) (result []*T, err error) {

	cls := d.readCollection

	if pagination != nil {
		total, err := cls.CountDocuments(ctx, filter)
		if err != nil {
			return nil, err
		}

		pagination.Init(int64(pagination.PageCurrent), int64(pagination.PageLimit))
		pagination.SetTotalRecord(total)

		opt.SetSkip(pagination.Offset)
		opt.SetLimit(pagination.PageLimit)
	}

	cur, err := cls.Find(ctx, filter, opt)
	if err != nil {
		return nil, err
	}

	err = cur.All(ctx, &result)
	if err != nil {
		return nil, err
	}

	return
}

func (d *implCollection[T]) FindOne(ctx context.Context, filter interface{}, opt *options.FindOneOptions) (*T, error) {
	bOpt := new(options.FindOneOptions)

	result := new(T)

	err := d.readCollection.
		FindOne(ctx, filter, bOpt).
		Decode(result)

	if err != nil {
		return nil, err
	}

	return result, err
}

func (d *implCollection[T]) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opt *options.FindOneAndUpdateOptions) (result *T, err error) {

	err = d.writeCollection.FindOneAndUpdate(ctx, filter, bson.M{"$set": update}, opt).Decode(result)

	if err != nil {
		return nil, err
	}

	return
}

func (d *implCollection[T]) EnsureIndexes(indexes []mongo.IndexModel) error {
	type MongoIndex struct {
		Name string
		Keys interface{}
	}

	var (
		dropIndexes = make([]string, 0)
	)

	c := d.writeCollection
	duration := 10 * time.Second
	batchSize := int32(100)

	cur, err := c.Indexes().List(context.Background(), &options.ListIndexesOptions{
		BatchSize: &batchSize,
		MaxTime:   &duration,
	})
	if err != nil {
		log.Fatalf("Something went wrong: %v", err)
	}

	sort.Slice(indexes, func(i, j int) bool {
		return *indexes[i].Options.Name <= *indexes[j].Options.Name
	})

	for cur.Next(context.Background()) {
		index := MongoIndex{}
		err = cur.Decode(&index)
		if err != nil {
			return err
		}

		if index.Name == "_id_" {
			continue
		}

		isDrop := true
		for _, v := range indexes {
			if *v.Options.Name == index.Name {
				isDrop = false
			}
		}

		// Drop all index is not found on which defined
		if isDrop {
			dropIndexes = append(dropIndexes, index.Name)
		}
	}

	// drop index
	for _, indexStr := range dropIndexes {
		opts := options.DropIndexes().SetMaxTime(10 * time.Second)
		_, err := d.writeCollection.Indexes().DropOne(context.Background(), indexStr, opts)
		if err != nil {
			return err
		}
	}

	// create index
	for _, index := range indexes {
		opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
		_, err := d.writeCollection.Indexes().CreateOne(context.Background(), index, opts)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *implCollection[T]) InsertOne(ctx context.Context, document T) error {
	_, err := d.writeCollection.InsertOne(ctx, document)
	return err
}

func (d *implCollection[T]) InsertMany(ctx context.Context, documents []*T) error {

	docs := make([]interface{}, len(documents))

	for i, v := range documents {
		docs[i] = v
	}

	_, err := d.writeCollection.InsertMany(ctx, docs)

	return err
}

func (d *implCollection[T]) UpdateOne(ctx context.Context, filter interface{}, update interface{}, options *options.UpdateOptions) error {

	if options.Hint == nil {
		return errors.New("miss hint index")
	}

	_, err := d.writeCollection.UpdateOne(ctx, filter, bson.M{"$set": update}, options)
	return err
}

func (d *implCollection[T]) UpdateMany(ctx context.Context, filter interface{}, update interface{}, options *options.UpdateOptions) error {

	if options.Hint == nil {
		return errors.New("miss hint index")
	}

	_, err := d.writeCollection.UpdateMany(ctx, filter, bson.M{"$set": update}, options)
	return err
}

func (d *implCollection[T]) Delete(ctx context.Context, filter interface{}, options *options.DeleteOptions) error {
	_, err := d.writeCollection.DeleteMany(ctx, filter, options)
	return err
}

func (d *implCollection[T]) Count(ctx context.Context, filter interface{}, opts *options.CountOptions) (int64, error) {
	return d.readCollection.CountDocuments(ctx, filter, opts)
}

func (d *implCollection[T]) BulkWrite(ctx context.Context, operations []mongo.WriteModel, opts *options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	return d.writeCollection.BulkWrite(ctx, operations, opts)
}
