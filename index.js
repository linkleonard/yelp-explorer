const fs = require('fs');
const assert = require('assert');
const es = require('event-stream');
const reduce = require('stream-reduce')

// argv[0] is the node executable, argv[1] is this script path. Therefore,
// the first meaningful argument is argv[2].
const consumeFilePath = process.argv[2];

assert(consumeFilePath, 'A file path must be provided!');

function count(accumulator, data) {
  return accumulator + 1;
}

function report(data, callback) {
  callback(null, `Processed ${data} lines.`);
}

fs.createReadStream(consumeFilePath)
  .pipe(es.split())
  .pipe(es.parse())
  .pipe(reduce(count, 0))
  .pipe(es.map(report))
  .pipe(process.stdout);
