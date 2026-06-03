import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { connect, ChannelModel, Channel } from 'amqplib';

@Injectable()
export class RabbitMQService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMQService.name);
  private connection!: ChannelModel;
  private channel!: Channel;

  private readonly exchange = 'orders';
  private readonly url = process.env.RABBITMQ_URL!;

  async onModuleInit() {
    await this.connect();
  }

  async onModuleDestroy() {
    await this.channel.close();
    await this.connection.close();
  }

  private async connect() {
    this.connection = await connect(this.url);
    this.channel = await this.connection.createChannel();
    await this.channel.assertExchange(this.exchange, 'topic', {
      durable: true,
    });
    this.logger.log('RabbitMQ connected');
  }

  publish(routingKey: string, payload: Record<string, any>) {
    const message = Buffer.from(JSON.stringify(payload));

    this.channel.publish(this.exchange, routingKey, message, {
      persistent: true,
      contentType: 'application/json',
      timestamp: Date.now(),
    });

    this.logger.log(`Published [${routingKey}]: ${JSON.stringify(payload)}`);
  }
}
