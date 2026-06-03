import Redis from 'ioredis';

const redis = new Redis({
  host: process.env.REDIS_HOST ?? 'localhost',
  port: Number(process.env.REDIS_PORT ?? 6379),
});

export async function testRedisConnection() {
  await redis.set('name', 'Ahmed');

  const value = await redis.get('name');

  console.log(value);

  return value;
}

export default redis;
