import Redis from 'ioredis';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const REDIS_PREFIX = process.env.REDIS_PREFIX || 'dataflow:';

let redis = null;
let isConnected = false;

function getRedisClient() {
  if (redis) return redis;

  redis = new Redis(REDIS_URL, {
    maxRetriesPerRequest: 3,
    retryStrategy(times) {
      if (times > 10) return null;
      return Math.min(times * 200, 5000);
    },
    lazyConnect: true,
    keyPrefix: REDIS_PREFIX,
    enableReadyCheck: true,
  });

  redis.on('connect', () => {
    isConnected = true;
  });

  let errorLogged = false;
  redis.on('error', (err) => {
    isConnected = false;
    if (!errorLogged) {
      errorLogged = true;
    }
  });

  redis.on('close', () => {
    isConnected = false;
  });

  return redis;
}

async function connectRedis() {
  const client = getRedisClient();
  try {
    await client.connect();
    isConnected = true;
  } catch (err) {
    isConnected = false;
  }
  return client;
}

function isRedisConnected() {
  return isConnected && redis?.status === 'ready';
}

async function setSession(sessionId, data, ttlSeconds) {
  if (isRedisConnected()) {
    await redis.set(`session:${sessionId}`, JSON.stringify(data), 'EX', ttlSeconds);
    return true;
  }
  return false;
}

async function getSession(sessionId) {
  if (isRedisConnected()) {
    const data = await redis.get(`session:${sessionId}`);
    return data ? JSON.parse(data) : null;
  }
  return null;
}

async function deleteSession(sessionId) {
  if (isRedisConnected()) {
    await redis.del(`session:${sessionId}`);
    return true;
  }
  return false;
}

async function cacheGet(key) {
  if (!isRedisConnected()) return null;
  const data = await redis.get(`cache:${key}`);
  return data ? JSON.parse(data) : null;
}

async function cacheSet(key, value, ttlSeconds = 300) {
  if (!isRedisConnected()) return false;
  await redis.set(`cache:${key}`, JSON.stringify(value), 'EX', ttlSeconds);
  return true;
}

async function cacheDel(key) {
  if (!isRedisConnected()) return false;
  await redis.del(`cache:${key}`);
  return true;
}

async function healthCheck() {
  if (!isRedisConnected()) return { status: 'disconnected' };
  try {
    const start = Date.now();
    await redis.ping();
    return { status: 'connected', latency_ms: Date.now() - start };
  } catch (err) {
    return { status: 'error', error: err.message };
  }
}

async function shutdownRedis() {
  if (redis) {
    await redis.quit();
    redis = null;
    isConnected = false;
  }
}

export {
  getRedisClient,
  connectRedis,
  isRedisConnected,
  setSession,
  getSession,
  deleteSession,
  cacheGet,
  cacheSet,
  cacheDel,
  healthCheck,
  shutdownRedis,
};
