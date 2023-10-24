package mongo

import (
	"context"
	"fmt"
	"io"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/rockset/rockset-mongo/pkg/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDump struct {
	ToolOptions *options.ToolOptions
	// InputOptions *InputOptions

	ProgressManager progress.Manager
	SessionProvider *db.SessionProvider

	isMongos     bool
	isAtlasProxy bool

	collection  *mongo.Collection
	dbNamespace string
}

// Init performs preliminary setup operations for MongoDump.
func (dump *MongoDump) Init() error {
	log.Logvf(log.DebugHigh, "initializing mongodump object")

	var err error
	// pref, err := db.NewReadPreference(dump.InputOptions.ReadPreference, dump.ToolOptions.URI.ParsedConnString())
	// if err != nil {
	// 	return fmt.Errorf("error parsing --readPreference : %v", err)
	// }
	// dump.ToolOptions.ReadPreference = pref

	dump.SessionProvider, err = db.NewSessionProvider(*dump.ToolOptions)
	if err != nil {
		return fmt.Errorf("can't create session: %v", err)
	}

	dump.isMongos, err = dump.SessionProvider.IsMongos()
	if err != nil {
		return fmt.Errorf("error checking for Mongos: %v", err)
	}

	dump.isAtlasProxy = dump.SessionProvider.IsAtlasProxy()
	if dump.isAtlasProxy {
		log.Logv(log.DebugLow, "dumping from a MongoDB Atlas free or shared cluster")
	}

	session, err := dump.SessionProvider.GetSession()
	if err != nil {
		return err
	}

	dump.collection = session.Database(dump.ToolOptions.DB).Collection(dump.ToolOptions.Collection)
	dump.dbNamespace = dump.ToolOptions.DB + "." + dump.ToolOptions.Collection

	// warn if we are trying to dump from a secondary in a sharded cluster
	if dump.isMongos && dump.ToolOptions.ReadPreference != readpref.Primary() {
		log.Logvf(log.Always, db.WarningNonPrimaryMongosConnection)
	}

	return nil
}

func (dump *MongoDump) Close() {
	dump.SessionProvider.Close()
}

func (dump *MongoDump) Dump(ctx context.Context, writer io.Writer) error {
	session, err := dump.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error getting a client session: %v", err)
	}
	err = session.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("error connecting to host: %v", err)
	}
	log.Logvf(log.DebugLow, "exporting collection")

	return dump.dumpCollection(ctx, writer)
}

func (dump *MongoDump) dumpCollection(ctx context.Context, writer io.Writer) error {
	query := &db.DeferredQuery{Coll: dump.collection}

	isMMAPV1, err := db.IsMMAPV1(dump.collection.Database(), dump.collection.Name())
	if err != nil {
		log.Logvf(log.Always,
			"failed to determine storage engine, an mmapv1 storage engine could result in"+
				" inconsistent dump results, error was: %v", err)
	} else if isMMAPV1 {
		log.Logvf(log.Always, "running with MMAP, setting _id hint")
		query.Hint = bson.D{{"_id", 1}}
	}

	dumpCount, err := dump.dumpValidatedQuery(ctx, query, writer)
	if err == nil {
		// on success, print the document count
		log.Logvf(log.Always, "dumped %v documents", dumpCount)
	}
	return err
}

// getCount counts the number of documents in the namespace for the given intent. It does not run the count for
// the oplog collection to avoid the performance issue in TOOLS-2068.
func (dump *MongoDump) getCount(query *db.DeferredQuery) (int64, error) {
	log.Logvf(log.DebugHigh, "Getting estimated count for %v.%v", query.Coll.Database().Name(), query.Coll.Name())
	total, err := query.Count(false)
	if err != nil {
		return 0, fmt.Errorf("error getting count from db: %v", err)
	}

	log.Logvf(log.DebugLow, "counted %v documents in %v", total, dump.dbNamespace)
	return int64(total), nil
}

// dumpValidatedQueryToIntent takes an mgo Query, its intent, a writer, and a document validator, performs the query,
// validates the results with the validator,
// and writes the raw bson results to the writer. Returns a final count of documents
// dumped, and any errors that occurred.
func (dump *MongoDump) dumpValidatedQuery(
	ctx context.Context, query *db.DeferredQuery, writer io.Writer) (dumpCount int64, err error) {

	total, err := dump.getCount(query)
	if err != nil {
		return 0, err
	}

	dumpProgressor := progress.NewCounter(total)
	if dump.ProgressManager != nil {
		dump.ProgressManager.Attach(dump.dbNamespace, dumpProgressor)
		defer dump.ProgressManager.Detach(dump.dbNamespace)
	}

	cursor, err := query.Iter()
	if err != nil {
		return
	}
	err = dump.dumpValidatedIterToWriter(ctx, cursor, writer, dumpProgressor)
	dumpCount, _ = dumpProgressor.Progress()
	if err != nil {
		err = fmt.Errorf("error writing data for collection `%v` to disk: %v", dump.dbNamespace, err)
	}
	return
}

func (dump *MongoDump) dumpValidatedIterToWriter(
	ctx context.Context, iter *mongo.Cursor, writer io.Writer, progressCount progress.Updateable) error {
	defer iter.Close(context.TODO())
	var termErr error

	// We run the result iteration in its own goroutine,
	// this allows disk i/o to not block reads from the db,
	// which gives a slight speedup on benchmarks
	buffChan := make(chan []byte)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Logvf(log.DebugHigh, "terminating writes")
				termErr = util.ErrTerminated
				close(buffChan)
				return
			default:
				if !iter.Next(ctx) {
					if err := iter.Err(); err != nil {
						termErr = err
					}
					close(buffChan)
					return
				}

				out := make([]byte, len(iter.Current))
				copy(out, iter.Current)
				buffChan <- out
			}
		}
	}()

	// while there are still results in the database,
	// grab results from the goroutine and write them to filesystem
	for {
		buff, alive := <-buffChan
		if !alive {
			if iter.Err() != nil {
				return fmt.Errorf("error reading collection: %v", iter.Err())
			}
			break
		}
		_, err := writer.Write(buff)
		if err != nil {
			return fmt.Errorf("error writing to file: %v", err)
		}
		progressCount.Inc(1)
	}
	return termErr
}

func (dump *MongoDump) CollectionInfo(ctx context.Context) (*config.CollectionInfo, error) {
	stream, err := dump.collection.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return nil, fmt.Errorf("failed to read changelog stream: %w", err)
	}

	defer stream.Close(context.TODO())

	if stream.TryNext(ctx) {
		log.Logvf(log.Always, "read %v", stream.Current.String())
	}

	r := dump.collection.Database().RunCommand(ctx, bson.M{"collStats": dump.collection.Name()})
	if r.Err() != nil {
		panic(r.Err())
	}
	var stats bson.M

	if err := r.Decode(&stats); err != nil {
		return nil, fmt.Errorf("error decoding collStats: %w", err)
	}

	info := &config.CollectionInfo{
		Documents:   toUint64(stats["count"]),
		Size:        toUint64(stats["size"]),
		ResumeToken: stream.ResumeToken().String(),
	}
	log.Logvf(log.Always, "collection stats %+v", info)
	return info, nil
}

func toUint64(value interface{}) uint64 {
	switch v := value.(type) {
	// all supported numerical types (except for complex)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)

	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)

	case float32:
		return uint64(v)
	case float64:
		return uint64(v)

	default:
		panic(fmt.Errorf("unsupported type: %T", value))
	}
}
