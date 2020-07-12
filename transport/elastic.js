const { Writable } = require('readable-stream');
const { Client } = require('@elastic/elasticsearch');
const pump = require('pump');
const logsplit = require('./logsplitter');

function toEcs(chunk) {
  const {
    err,
    time,
    msg,
    level,
    hostname,
    pid,
    req,
    res,
    stack,
  } = chunk;
  const log = { };
  if (chunk.log_from) {
    log.source = chunk.log_from;
  }

  if (err || stack) {
    log.error = log.error || {};
    if (err) {
      log.error.code = err.type;
      log.error.message = err.message;
      // `error.stack` is not standardized
      log.error.stack = err.stack;
    } else {
      log.error.message = msg;
      log.error.stack = stack;
    }
  }

  if (time) {
    log['@timestamp'] = time;
  }

  if (msg) {
    log.message = msg;
  }

  if (level) {
    log.log = {};
    log.log.level = level;
  }

  if (hostname) {
    log.host = log.host || {};
    log.host.hostname = hostname;
  }

  if (pid) {
    log.pid = pid;
  }

  if (req) {
    const {
      id,
      method,
      url,
      remoteAddress,
      remotePort,
      headers,
    } = req;

    if (id) {
      log.event = log.event || {};
      log.event.id = id;
    }

    log.http = log.http || {};
    log.http.request = log.http.request || {};
    log.http.request.method = method;

    log.url = log.url || {};
    log.url.path = url;

    if (hostname) {
      const [
        host,
        port,
      ] = hostname.split(':');
      log.url.domain = host;
      if (port) {
        log.url.port = Number(port);
      }
    }

    log.client = log.client || {};
    log.client.address = remoteAddress;
    log.client.port = remotePort;

    if (headers) {
      if (headers['user-agent']) {
        log.user_agent = log.user_agent || {};
        log.user_agent.original = headers['user-agent'];
        delete headers['user-agent'];
      }
      if (headers['content-length']) {
        log.http.request.body = log.http.request.body || {};
        log.http.request.body.bytes = Number(headers['content-length']);
        delete headers['content-length'];
      }

      if (Object.keys(headers).length) {
        // `http.request.headers` is not standardized
        log.http.request.headers = headers;
      }
    }
  }

  if (res) {
    const { statusCode, headers } = res;
    log.http = log.http || {};
    log.http.response = log.http.response || {};
    log.http.response.status_code = statusCode;

    if (headers) {
      if (headers['content-length']) {
        log.http.response.body = log.http.response.body || {};
        log.http.response.body.bytes = Number(headers['content-length']);
        delete headers['content-length'];
      }

      if (Object.keys(headers).length) {
        // `http.response.headers` is not standardized
        log.http.response.headers = headers;
      }
    }
  }

  return log;
}

function plainLogDoc(doc, stdoutLog) {
  if (doc.error) {
    stdoutLog(doc.message, doc.error.stack);
  } else {
    stdoutLog(`${doc.event ? doc.event.id : ''}${doc.event ? ' ' : ''}${doc.message}`);
  }
}

function pinoElasticSearch({
  node, index, bulkSize, stdoutLog,
}) {
  const logsplitter = logsplit();
  const client = new Client({ node });
  const fallbackLog = console.log; // eslint-disable-line
  const writable = new Writable({
    objectMode: true,
    highWaterMark: bulkSize || 500,
    writev(chunks, cb) {
      const docs = [];
      for (let i = 0; i < chunks.length; i += 1) {
        const { chunk } = chunks[i];
        // Add the header
        const docIdx = {
          index: {
            _index: `${index}-${chunk.time.substring(
              0,
              10,
            )}`,
          },
        };
        const doc = toEcs(chunk);
        if (stdoutLog) {
          plainLogDoc(doc, stdoutLog);
        }
        docs.push(
          docIdx,
          doc,
        );
      }
      client.bulk(
        {
          body: docs,
        },
        (err, result) => {
          if (!err) {
            const { items } = result.body;
            for (let i = 0; i < items.length; i += 1) {
              const create = items[i].index;
              logsplitter.emit(
                'insert',
                create,
                docs[i * 2 + 1],
              );
            }
          } else {
            // logsplitter.emit(
            //  'insertError',
            //  err,
            // );
            if (!stdoutLog) {
              for (let i = 1; i < docs.length; i += 2) {
                plainLogDoc(docs[i], fallbackLog);
              }
            }
            fallbackLog('elasticlog bulk err:', err.message);
          }
          // Skip error and continue how to resend
          cb();
        },
      );
    },
    write(body, enc, cb) {
      const obj = {
        index: `${index}-${body.time.substring(
          0,
          10,
        )}`,
        body: toEcs(body),
      };
      if (stdoutLog) {
        plainLogDoc(obj.body, stdoutLog);
      }
      client.index(
        obj,
        (err, data) => {
          if (!err) {
            logsplitter.emit(
              'insert',
              data.body,
              obj.body,
            );
          } else {
            // logsplitter.emit(
            //  'insertError',
            //  err,
            // );
            if (!stdoutLog) {
              plainLogDoc(obj.body, fallbackLog);
            }
            fallbackLog('elasticlog index err:', err.message);
          }
          // Skip error and continue
          cb();
        },
      );
    },
  });

  pump([
    logsplitter,
    writable,
  ]);

  return logsplitter;
}

module.exports = pinoElasticSearch;
