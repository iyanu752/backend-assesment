import { Module } from '@nestjs/common';
import { RabbitMQService } from './rabbitmq.service';
import { OrderConsumer } from './order.consumer';

@Module({
  providers: [RabbitMQService, OrderConsumer],
  exports: [RabbitMQService],
})
export class RabbitmqModule {}
