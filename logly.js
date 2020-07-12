const pino = require('pino');
const serializers = require('pino-std-serializers');
const bytes = require('bytes');
const chalk = require('chalk');

const elasticTransport = require('./transport/elastic');
const rawTransport = require('./transport/rawout');

function logger({
  level, name, redact, base, bufferLimit, stdoutLog, elastic,
}) {
  let transport;
  if (elastic) {
    transport = elasticTransport({
      node: elastic.node, index: elastic.index, bulkSize: bufferLimit, stdoutLog,
    });
  } else {
    transport = rawTransport({ bulkSize: bufferLimit, stdoutLog });
  }
  return pino({
    level, name, redact, base: base || null, useLevelLabels: true,
  }, transport);
}

function koaLogger(httpLogger) {
  const optSerializer = {
    req: serializers.wrapRequestSerializer(serializers.req),
    res: serializers.wrapResponseSerializer(serializers.res),
    err: serializers.wrapErrorSerializer(serializers.err),
  };
  const maxInt = 2147483647;
  let nextReqId = 0;
  /**
 * Color map.
 */

  const colorCodes = {
    5: 'red',
    4: 'yellow',
    3: 'cyan',
    2: 'green',
    1: 'green',
    0: 'yellow',
  };

  /**
 * Show the response time in a human readable format.
 * In milliseconds if less than 10 seconds,
 * in seconds otherwise.
 */

  function time(start) {
    let delta = new Date() - start;
    delta = delta < 10000
      ? `${delta}ms`
      : `${Math.round(delta / 1000)}s`;
    return delta;
  }
  /**
 * Log helper.
 */

  function log(ctx, start, len, err, event) {
  // get the status code of the response
    let status = (ctx.status || 404);
    let upstream;
    if (err) {
      status = (err.status || 500);
      upstream = chalk.red('xxx');
    } else if (event === 'close') {
      upstream = chalk.yellow('-x-');
    } else {
      upstream = chalk.gray('-->');
    }

    // set the color of the status code;
    const color = colorCodes[(status / 100) | 0]; // eslint-disable-line

    // get the human readable response length
    let length;
    if ([204, 205, 304].indexOf(status) !== -1) {
      length = '';
    } else if (len == null) {
      length = '-';
    } else {
      length = bytes(len);
    }

    ctx.log.info(`  ${upstream} ${chalk.bold(ctx.method)} ${chalk.gray(ctx.originalUrl)} ${chalk[color](status)} ${chalk.gray(time(start))} ${chalk.gray(length)}`);
  }
  function genReqId(req) {
    if (req.id) {
      return req.id;
    }
    nextReqId += 1;
    return nextReqId & maxInt; // eslint-disable-line no-bitwise
  }
  return function* koaLogMw(next) {
    this.req.id = genReqId(this.req);
    this.log = httpLogger.child({
      serializers: optSerializer,
      req: this.req,
    });
    const start = new Date();
    this.log.info(`  ${chalk.gray('<--')} ${chalk.bold(this.method)} ${chalk.gray(this.originalUrl)}`);
    try {
      yield next;
    } catch (err) {
      // log uncaught downstream errors
      log(this, start, null, err, null);
      throw err;
    }

    // calculate the length of a streaming response
    // by intercepting the stream with a counter.
    // only necessary if a content-length header is currently not set.
    const { length } = this.response;

    // log when the response is finished or closed,
    // whichever happens first.
    const ctx = this;
    const { res } = this;
    const onfinish = onResDone.bind(null, 'finish'); // eslint-disable-line
    const onclose = onResDone.bind(null, 'close');// eslint-disable-line
    res.once('finish', onfinish);
    res.once('close', onclose);

    function onResDone(event) {
      res.removeListener('finish', onfinish);
      res.removeListener('close', onclose);
      log(ctx, start, length, null, event);
    }
  };
}

module.exports = {
  logger,
  koaLogger,
};
