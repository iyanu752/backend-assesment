import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import { connect, ChannelModel, Channel } from 'amqplib';

@Injectable()
export class OrderConsumer implements OnModuleInit {
  private readonly logger = new Logger(OrderConsumer.name);

  private readonly exchange = 'orders';
  private readonly queue = 'order.notifications';
  private readonly routingKey = 'order.created';
  private readonly url = process.env.RABBITMQ_URL!;
  private connection!: ChannelModel;
  private channel!: Channel;
  async onModuleInit() {
    await this.startConsuming();
  }

  private async startConsuming() {
    this.connection = await connect(this.url);
    this.channel = await this.connection.createChannel();

    await this.channel.assertExchange(this.exchange, 'topic', {
      durable: true,
    });

    // assertQueue with durable:true — queue survives broker restarts
    await this.channel.assertQueue(this.queue, { durable: true });

    // Bind the queue to the exchange with the routing key pattern
    await this.channel.bindQueue(this.queue, this.exchange, this.routingKey);

    // prefetch(1) — process one message at a time; don't overwhelm the worker
    await this.channel.prefetch(1);

    this.logger.log(`Listening on queue: ${this.queue}`);

    await this.channel.consume(this.queue, (msg) => {
      void (async () => {
        if (!msg) return;

        try {
          const payload = JSON.parse(msg.content.toString()) as {
            orderId: string;
            customerId: string;
            totalAmount: number;
            itemCount: number;
          };
          await this.handleOrderCreated(payload);
          this.channel.ack(msg);
        } catch (error) {
          this.logger.error('Failed to process message', error);
          this.channel.nack(msg, false, false);
        }
      })();
    });
  }

  private async handleOrderCreated(payload: {
    orderId: string;
    customerId: string;
    totalAmount: number;
    itemCount: number;
  }) {
    this.logger.log(
      `Processing order.created for orderId=${payload.orderId}, customer=${payload.customerId}`,
    );

    // Simulate sending a confirmation email
    await this.sendConfirmationEmail(payload);
  }

  private async sendConfirmationEmail(payload: {
    orderId: string;
    customerId: string;
    totalAmount: number;
    itemCount: number;
  }) {
    // In production: inject a MailService (Nodemailer / SendGrid / SES)
    // and call mailerService.send({ to, subject, template })
    await new Promise((resolve) => setTimeout(resolve, 100)); // simulated I/O

    this.logger.log(
      `Email sent to customer ${payload.customerId} for order ${payload.orderId} — ` +
        `Total: $${payload.totalAmount}, Items: ${payload.itemCount}`,
    );
  }
}
