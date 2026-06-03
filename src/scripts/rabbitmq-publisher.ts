import { publishEmailMessage } from '../config/rabbitmq.config';

async function bootstrap() {
  await publishEmailMessage();
}

void bootstrap();
