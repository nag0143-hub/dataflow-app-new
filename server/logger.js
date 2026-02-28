import pino from 'pino';

const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const NODE_ENV = process.env.NODE_ENV || 'development';

const logger = pino({
  level: LOG_LEVEL,
  timestamp: pino.stdTimeFunctions.isoTime,
  base: {
    service: 'dataflow',
    env: NODE_ENV,
    pid: process.pid,
  },
  formatters: {
    level(label) {
      return { level: label };
    },
  },
  ...(NODE_ENV === 'development'
    ? {
        transport: {
          target: 'pino/file',
          options: { destination: 1 },
        },
      }
    : {}),
});

export function createHttpLogger() {
  return (req, res, next) => {
    if (!req.url.startsWith('/api')) return next();
    const start = Date.now();
    const reqId = req.headers['x-request-id'] || generateReqId();
    req.reqId = reqId;
    res.setHeader('x-request-id', reqId);

    res.on('finish', () => {
      const duration = Date.now() - start;
      const logData = {
        reqId,
        method: req.method,
        url: req.url,
        statusCode: res.statusCode,
        duration_ms: duration,
        contentLength: res.getHeader('content-length'),
        userAgent: req.headers['user-agent'],
        ip: req.ip || req.connection?.remoteAddress,
      };

      if (res.statusCode >= 500) {
        logger.error(logData, 'request failed');
      } else if (res.statusCode >= 400) {
        logger.warn(logData, 'request client error');
      } else {
        logger.info(logData, 'request completed');
      }
    });

    next();
  };
}

let counter = 0;
function generateReqId() {
  return `req-${Date.now().toString(36)}-${(counter++).toString(36)}`;
}

export default logger;
