import { consumeEmailMessages } from '../config/rabbitmq.config';

async function bootstrap() {
  await consumeEmailMessages();
}

void bootstrap();
