const fs = require('fs');
const assert = require('assert');
const es = require('event-stream');
const reduce = require('stream-reduce')
const MongoClient = require('mongodb').MongoClient;
const config = require('dotenv').config();
const { ObjectChunker } = require('./streams');

assert.equal(undefined, config.error, config.error);


// argv[0] is the node executable, argv[1] is this script path. Therefore,
// the first meaningful argument is argv[2].
const collectionName = process.argv[2];
const consumeFilePath = process.argv[3];

assert(consumeFilePath, 'A file path must be provided!');
assert(collectionName, 'A collection name must be provided!');

function count(accumulator, data) {
  return accumulator + data;
}

function report(data, callback) {
  callback(null, `Processed ${data} lines.`);
}


function getStreamRowImporter(collection) {
  return function(data, callback) {
    // We need not return a promise, as we're dealing with streams here.
    // However, we do need to notify map() that we are done processing this
    // record. We'll do that by calling callback() when handling the promise
    // returned by collection.insertOne().
    collection.insertOne(data).then(
      (result) => {
        callback(null, result.result.n);
      },
      (error) => {
        callback(error);
      })
  }
}


function getStreamBatchImporter(collection) {
  return function(data, callback) {
    // We need not return a promise, as we're dealing with streams here.
    // However, we do need to notify map() that we are done processing these
    // records. We'll do that by calling callback() when handling the promise
    // returned by collection.insertOne().

    // TODO: We should deal with rows that failed to insert, due to a duplicate
    // key conflict. The logical thing to do is to update the value stored with
    // the value we have. We'll just currently have the server generate IDs for
    // us, ensuring that we insert unique values.
    collection.insertMany(data, {forceServerObjectId: true, ordered: false}).then(
      (result) => {
        callback(null, result.result.n);
      },
      (error) => {
        callback(error);
      })
  }
}


MongoClient.connect(process.env.DATABASE_URL)
  .then((db) => {
    const collection = db.collection(collectionName);
    fs.createReadStream(consumeFilePath)
      .pipe(es.split())
      .pipe(es.parse())
      .pipe(new ObjectChunker({ chunksize: 4096 }))
      .pipe(es.map(getStreamBatchImporter(collection)))
      .on('error', (error) => {
        console.error(`Received exception while importing chunks: "${error}"`);
        db.close();
      })
      .on('close', () => {
        console.log('Record parsing complete!');
        collection.stats().then((stats) => {
          console.log(`Collection now contains: ${stats.count} objects`);
        }).then(
          // Close the database connection, regardless if the stats retrieval
          // succeeded or failed.
          () => db.close(),
          () => db.close(),
        );
      })
      .pipe(reduce(count, 0))
      .pipe(es.map(report))
      .pipe(process.stdout);
  });
