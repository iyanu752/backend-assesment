import amqp from 'amqplib';

const rabbitMqUrl =
  process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';

export const emailQueue = 'emails';

export async function createRabbitMqChannel() {
  const connection = await amqp.connect(rabbitMqUrl);
  const channel = await connection.createChannel();

  await channel.assertQueue(emailQueue);

  return { connection, channel };
}

export async function publishEmailMessage(email = 'test@test.com') {
  const { channel, connection } = await createRabbitMqChannel();

  channel.sendToQueue(
    emailQueue,
    Buffer.from(
      JSON.stringify({
        email,
      }),
    ),
  );

  await channel.close();
  await connection.close();
}

export async function consumeEmailMessages() {
  const { channel, connection } = await createRabbitMqChannel();

  await channel.consume(emailQueue, (msg) => {
    if (!msg) {
      return;
    }

    console.log(msg.content.toString());

    channel.ack(msg);
  });

  return { channel, connection };
}
