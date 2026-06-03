import redis, { testRedisConnection } from '../config/redis.config';

async function bootstrap() {
  await testRedisConnection();
  redis.disconnect();
}

void bootstrap();
