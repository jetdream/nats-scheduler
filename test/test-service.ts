/* eslint-disable prettier/prettier */
require('dotenv').config();
import { AckPolicy, connect, ConsumerConfig, JetStreamClient, JetStreamManager, NatsConnection } from 'nats';

import { envStr } from '../src/utils';
import { v4 as uuidv4 }  from 'uuid';
import { SchedulerCommandCancel, SchedulerCommandOneOffSchedule, SchedulerCommandRecurringSchedule } from '../src/Scheduler';

// Gateway parameters
const NATS_SERVERS_CONFIG = envStr('NATS_SERVERS_CONFIG', 'NATS servers configuration');


async function test() {
  const conn: NatsConnection = await connect({ servers: NATS_SERVERS_CONFIG.split(',') });
  const jsm: JetStreamManager = await conn.jetstreamManager();
  const js: JetStreamClient | null = conn.jetstream();

  const publish = async (subject: string, data: any) => {
    try {
      data = data ?? {};
      data = {
        id: uuidv4().slice(0, 4),
        origin: 'another-service',
        timestamp: +new Date(),
        ...data,
      };
      await js!.publish(subject, JSON.stringify(data));
    } catch (err) {
      throw new Error('Error publishing message to NATS');
    }
  }

  const consumerConfig: Partial<ConsumerConfig> = {
    durable_name: 'test123',
    ack_policy: AckPolicy.Explicit,
    ack_wait: 10 * 1000_000_000, // 10 seconds
  };
  await jsm!.consumers.add('nats-scheduler-1', consumerConfig);

  const consumer = await js!.consumers.get('nats-scheduler-1', consumerConfig.durable_name!);
  const consumerMessages = await consumer.consume();
  (async () => {
    for await (const m of consumerMessages) {
      console.log(`TEST Received: ${m.seq} ${m?.subject} ${m.data.toString()}`);
      m.ack();
    }
  })();

  const tests = {
    schedule: async () => {
      // send template message
      const command: SchedulerCommandOneOffSchedule = {
        recurring: false,
        subscriptionId: 'o123',
        scheduleAt: +new Date() + 3000,
        notify: {
          subject: 'nats-scheduler-1.event.service.notify',
          message: {
            payload: {
              message: 'Hello, world!'
            }
          }
        }
      };
      await publish('nats-scheduler-1.command.scheduler.schedule', command);
    },
    scheduleRecurring: async () => {
      // send template message
      const command: SchedulerCommandRecurringSchedule = {
        recurring: true,
        subscriptionId: 'r456',
        cronPattern: '* * * * * *',
        timezone: 'America/Los_Angeles',
        notify: {
          subject: 'nats-scheduler-1.event.service.notify',
          message: {
            payload: {
              message: 'Hello, recurring world!'
            }
          }
        }
      };
      await publish('nats-scheduler-1.command.scheduler.schedule', command);
    },
    cancel: async () => {
      await publish('nats-scheduler-1.command.scheduler.cancel', {subscriptionId: 'o123'});
      await publish('nats-scheduler-1.command.scheduler.cancel', {subscriptionId: 'r456'});
    },
    resume: async () => {
      await publish('nats-scheduler-1.command.service.resume',
        {
        }
      );
    },
    discovery: async () => {
      await conn.publish('services.discovery.request', JSON.stringify({id: uuidv4()}));
    }
  }

  const firstArgument = process.argv[2] as keyof typeof tests;
  if (!firstArgument) {
    console.error('Please provide a test name');
    process.exit(1);
  }

  const asyncFunc = tests[firstArgument] as () => Promise<void>;
  await asyncFunc();

  // delay for 5 seconds
  await new Promise((resolve) => setTimeout(resolve, 5000));


  console.log('Executed successfully! '+firstArgument);
  await conn.drain();
  process.exit(0);

}

test();
