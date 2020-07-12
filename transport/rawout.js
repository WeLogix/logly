const { Writable } = require('readable-stream');
const pump = require('pump');
const logsplit = require('./logsplitter');

function toLog(chunk) {
  const {
    err,
    msg,
    req,
  } = chunk;
  const log = {};
  if (msg) {
    log.message = msg;
  }

  if (err) {
    log.stack = err.stack;
  }
  if (req) {
    log.id = req.id;
  }
  return log;
}

function pinoRawOutput({
  bulkSize,
}) {
  const splitter = logsplit();
  const fallbackLog = console.log; // eslint-disable-line
  const writable = new Writable({
    objectMode: true,
    highWaterMark: bulkSize || 500,
    writev(chunks, cb) {
      for (let i = 0; i < chunks.length; i += 1) {
        const log = toLog(chunks[i]);
        fallbackLog(`${log.id || ''}${log.id ? ' ' : ''}${log.message || ''}${log.stack || ''}`);
      }
      cb();
    },
    write(body, enc, cb) {
      const log = toLog(body);
      fallbackLog(`${log.id || ''}${log.id ? ' ' : ''}${log.message || ''}${log.stack || ''}`);
      cb();
    },
  });

  pump([
    splitter,
    writable,
  ]);

  return splitter;
}

module.exports = pinoRawOutput;
