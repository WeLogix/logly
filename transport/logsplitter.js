const split = require('split2');

function setDateTimeString(value) {
  if (typeof value === 'object' && value.hasOwnProperty('time')) { // eslint-disable-line no-prototype-builtins
    if (
      (typeof value.time === 'string' && value.time.length)
          || (typeof value.time === 'number' && value.time >= 0)
    ) {
      return new Date(value.time).toISOString();
    }
  }
  return new Date().toISOString();
}

const logsplit = () => split((line) => {
  try {
    let value = JSON.parse(line);
    if (typeof value === 'boolean') {
      this.emit(
        'unknown',
        line,
        'Boolean value ignored',
      );
      return undefined;
    }
    if (value === null) {
      this.emit(
        'unknown',
        line,
        'Null value ignored',
      );
      return undefined;
    }
    if (typeof value !== 'object') {
      value = {
        msg: value,
        time: setDateTimeString(value),
      };
    } else {
      value.time = setDateTimeString(value);
    }
    return value;
  } catch (err) {
    this.emit(
      'unknown',
      line,
      err,
    );
    return undefined;
  }
});

module.exports = logsplit;
