const { Transform } = require('stream');

class ObjectChunker extends Transform {
  constructor(options) {
    options.objectMode = true;
    super(options);

    this.chunksize = options.chunksize || 1;
    this.buffer = new Array(this.chunksize);
    this.index = 0;
  }

  _transform(chunk, encoding, callback) {
    this.buffer[this.index] = chunk;
    ++this.index;

    if (this.index === this.chunksize) {
      this.push(this.buffer);
      this.index = 0;
    }
    callback();
  }

  // Buffer may not yet be full when the input stream ends, so we must be
  // sure to flush out any objects currently stored in our buffer.
  _flush(callback) {
    this.push(this.buffer);
    callback();
  }
}


module.exports = {
  ObjectChunker,
};
