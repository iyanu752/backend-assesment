import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import { connect, ChannelModel, Channel } from 'amqplib';
// import { AuditService } from '../audit/audit.service';
// import { AuditAction } from '../audit/audit-log.entity';

@Injectable()
export class TransferConsumer implements OnModuleInit {
  private readonly logger = new Logger(TransferConsumer.name);

  private readonly exchange = 'payments';
  private readonly queue = 'transfer.notifications';
  private readonly routingKey = 'transfer.completed';
  private readonly url =
    process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
  private connection!: ChannelModel;
  private channel!: Channel;
  constructor() {
    // private readonly auditService: AuditService
  }

  async onModuleInit() {
    await this.startConsuming();
  }

  private async startConsuming() {
    this.connection = await connect(this.url);
    this.channel = await this.connection.createChannel();

    await this.channel.assertExchange(this.exchange, 'topic', {
      durable: true,
    });
    await this.channel.assertQueue(this.queue, { durable: true });
    await this.channel.bindQueue(this.queue, this.exchange, this.routingKey);

    await this.channel.prefetch(1);
    this.logger.log(`Listening on queue: ${this.queue}`);

    await this.channel.consume(this.queue, (msg) => {
      void (async () => {
        if (!msg) return;

        try {
          const payload = JSON.parse(msg.content.toString()) as {
            transactionId: string;
            senderId: string;
            receiverId: string;
            amount: number;
            currency: string;
          };
          await this.handleTransferCompleted(payload);
          this.channel.ack(msg);
        } catch (error) {
          this.logger.error('Notification processing failed', error);
          this.channel.nack(msg, false, false); // → dead letter queue
        }
      })();
    });
  }

  private async handleTransferCompleted(payload: {
    transactionId: string;
    senderId: string;
    receiverId: string;
    amount: number;
    currency: string;
  }) {
    this.logger.log(
      `Processing transfer.completed for tx=${payload.transactionId}`,
    );

    // Send sender notification
    await this.sendNotification(payload.senderId, {
      type: 'debit',
      message: `Your transfer of ${payload.amount} ${payload.currency} was successful`,
      txId: payload.transactionId,
    });

    // Send receiver notification
    await this.sendNotification(payload.receiverId, {
      type: 'credit',
      message: `You received ${payload.amount} ${payload.currency}`,
      txId: payload.transactionId,
    });

    // Write notification sent to audit log
    // await this.auditService.log(
    //   AuditAction.NOTIFICATION_SENT,
    //   payload.transactionId,
    //   'transaction',
    //   null,
    //   { sentTo: [payload.senderId, payload.receiverId] },
    // );
  }

  private async sendNotification(
    userId: string,
    data: { type: string; message: string; txId: string },
  ) {
    // Production: inject NotificationService → push, email, or SMS
    await new Promise((r) => setTimeout(r, 50)); // simulated I/O
    this.logger.log(`Notification sent to ${userId}: ${data.message}`);
  }
}
