package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/progress"
	"github.com/mongodb/mongo-tools/common/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDump struct {
	ToolOptions  *options.ToolOptions
	InputOptions *InputOptions

	ProgressManager progress.Manager
	SessionProvider *db.SessionProvider
	query           bson.D
	isMongos        bool
	isAtlasProxy    bool

	db          string
	collection  string
	dbNamespace string

	// shutdownIntentsNotifier is provided to the multiplexer
	// as well as the signal handler, and allows them to notify
	// the intent dumpers that they should shutdown
	shutdownIntentsNotifier *notifier

	// Writer to take care of BSON output when not writing to the local filesystem.
	// This is initialized to os.Stdout if unset.
	OutputWriter io.Writer
}

type notifier struct {
	notified chan struct{}
	once     sync.Once
}

func (n *notifier) Notify() { n.once.Do(func() { close(n.notified) }) }

func newNotifier() *notifier { return &notifier{notified: make(chan struct{})} }

// Init performs preliminary setup operations for MongoDump.
func (dump *MongoDump) Init() error {
	log.Logvf(log.DebugHigh, "initializing mongodump object")

	pref, err := db.NewReadPreference(dump.InputOptions.ReadPreference, dump.ToolOptions.URI.ParsedConnString())
	if err != nil {
		return fmt.Errorf("error parsing --readPreference : %v", err)
	}
	dump.ToolOptions.ReadPreference = pref

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

	if dump.OutputWriter == nil {
		dump.OutputWriter = os.Stdout
	}

	dump.db = dump.ToolOptions.DB
	dump.collection = dump.ToolOptions.Collection
	dump.dbNamespace = dump.db + "." + dump.collection

	// warn if we are trying to dump from a secondary in a sharded cluster
	if dump.isMongos && pref != readpref.Primary() {
		log.Logvf(log.Always, db.WarningNonPrimaryMongosConnection)
	}

	return nil
}

func (dump *MongoDump) HandleInterrupt() {
	if dump.shutdownIntentsNotifier != nil {
		dump.shutdownIntentsNotifier.Notify()
	}
}

func (dump *MongoDump) Dump() error {
	defer dump.SessionProvider.Close()

	session, err := dump.SessionProvider.GetSession()
	if err != nil {
		return fmt.Errorf("error getting a client session: %v", err)
	}
	err = session.Ping(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("error connecting to host: %v", err)
	}
	log.Logvf(log.DebugLow, "exporting collection")

	dump.shutdownIntentsNotifier = newNotifier()

	return dump.dumpCollection()
}

func (dump *MongoDump) dumpCollection() error {
	session, err := dump.SessionProvider.GetSession()
	if err != nil {
		return err
	}
	intendedDB := session.Database(dump.db)
	coll := intendedDB.Collection(dump.collection)

	findQuery := &db.DeferredQuery{Coll: coll}
	if len(dump.query) > 0 {
		findQuery.Filter = dump.query
	}

	var dumpCount int64

	if true {
		log.Logvf(log.Always, "writing %v.%v to stdout", dump.db, dump.collection)
		dumpCount, err = dump.dumpValidatedQuery(findQuery, os.Stdout)
		if err == nil {
			// on success, print the document count
			log.Logvf(log.Always, "dumped %v documents", dumpCount)
		}
		return err
	}

	log.Logvf(log.Always, "writing %v to %v", dump.collection, "TODO")
	if dumpCount, err = dump.dumpValidatedQuery(findQuery, os.Stdout); err != nil {
		return err
	}

	log.Logvf(log.Always, "done dumping %v (%v documents)", dump.db, dumpCount)
	return nil
}

// getCount counts the number of documents in the namespace for the given intent. It does not run the count for
// the oplog collection to avoid the performance issue in TOOLS-2068.
func (dump *MongoDump) getCount(query *db.DeferredQuery) (int64, error) {
	if len(dump.query) != 0 {
		log.Logvf(log.DebugLow, "not counting query")
		return 0, nil
	}

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
	query *db.DeferredQuery, writer io.Writer) (dumpCount int64, err error) {

	// restore of views from archives require an empty collection as the trigger to create the view
	// so, we open here before the early return if IsView so that we write an empty collection to the archive
	// err = intent.BSONFile.Open()
	// if err != nil {
	// 	return 0, err
	// }
	// defer func() {
	// 	closeErr := intent.BSONFile.Close()
	// 	if err == nil && closeErr != nil {
	// 		err = fmt.Errorf("error writing data for collection `%v` to disk: %v", dump.dbNamespace, closeErr)
	// 	}
	// }()

	total, err := dump.getCount(query)
	if err != nil {
		return 0, err
	}

	info, err := dump.getCollectionInfo(query)
	if err != nil {
		return 0, err
	}
	log.Logvf(log.Always, "resume token: %v", info)

	dumpProgressor := progress.NewCounter(total)
	if dump.ProgressManager != nil {
		dump.ProgressManager.Attach("namespace", dumpProgressor)
		defer dump.ProgressManager.Detach("namespace")
	}

	// var f io.Writer
	// f = intent.BSONFile
	// if buffer != nil {
	// 	buffer.Reset(f)
	// 	f = buffer
	// 	defer func() {
	// 		closeErr := buffer.Close()
	// 		if err == nil && closeErr != nil {
	// 			err = fmt.Errorf("error writing data for collection `%v` to disk: %v", dump.dbNamespace, closeErr)
	// 		}
	// 	}()
	// }

	cursor, err := query.Iter()
	if err != nil {
		return
	}
	err = dump.dumpValidatedIterToWriter(cursor, writer, dumpProgressor)
	dumpCount, _ = dumpProgressor.Progress()
	if err != nil {
		err = fmt.Errorf("error writing data for collection `%v` to disk: %v", dump.dbNamespace, err)
	}
	return
}

func (dump *MongoDump) dumpValidatedIterToWriter(
	iter *mongo.Cursor, writer io.Writer, progressCount progress.Updateable) error {
	defer iter.Close(context.Background())
	var termErr error

	// We run the result iteration in its own goroutine,
	// this allows disk i/o to not block reads from the db,
	// which gives a slight speedup on benchmarks
	buffChan := make(chan []byte)
	go func() {
		ctx := context.Background()
		for {
			select {
			case <-dump.shutdownIntentsNotifier.notified:
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

func (dump *MongoDump) getCollectionInfo(query *db.DeferredQuery) (*CollectionInfo, error) {
	stream, err := query.Coll.Watch(context.TODO(), mongo.Pipeline{})
	if err != nil {
		return nil, fmt.Errorf("failed to read changelog stream: %w", err)
	}

	defer stream.Close(context.TODO())

	if stream.TryNext(context.TODO()) {
		log.Logvf(log.Always, "read %v", stream.Current.String())
	}

	r := query.Coll.Database().RunCommand(context.TODO(), bson.M{"collStats": query.Coll.Name()})
	if r.Err() != nil {
		panic(r.Err())
	}
	var stats bson.M

	if err := r.Decode(&stats); err != nil {
		return nil, fmt.Errorf("error decoding collStats: %w", err)
	}

	info := &CollectionInfo{
		Documents:   toUint64(stats["count"]),
		Size:        toUint64(stats["size"]),
		ResumeToken: stream.ResumeToken().String(),
	}
	log.Logvf(log.Always, "collection stats %+v", info)
	return info, nil
}
