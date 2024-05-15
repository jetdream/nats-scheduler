/* eslint-disable no-empty */
/* eslint-disable quotes */
/* eslint-disable prettier/prettier */
import {AckPolicy, connect, ConnectionOptions, Consumer, ConsumerConfig, ConsumerEvents, ConsumerMessages, JetStreamClient, JetStreamManager, JetStreamPublishOptions, JsMsg, NatsConnection, StreamConfig, Subscription} from 'nats';
import { IEventPublisher, IMessageProcessor, IncorrectMessageError } from './types';
import { v4 as uuidv4 }  from 'uuid';
import { debugLog } from './utils';

export interface SchedulerServiceConfig {
  serviceId: string;
  allowCreateServiceStream: boolean;
  failuresLimit: number;
}

export class SchedulerService implements IEventPublisher {
  private conn: NatsConnection | null = null;
  private jsm: JetStreamManager | null = null;
  private js: JetStreamClient | null = null;
  private consumers: Consumer[] = [];
  private consumersMessages: ConsumerMessages[] = [];
  private consumersStopped = 0;
  private terminated = false;
  private allConsumersStoppedCallback: (() => void) | null = null;
  private consequentialFailures = 0;
  private terminatedCallback: (() => void) | null = null;

  private telemetrySubscription: Subscription | undefined;
  public stopped = false; // indicates if the stop() method was called
  public paused = false; // indicates if the pause() method was called

  private serviceSubjects = {
    started: `${this.config.serviceId}.event.service.started`,
    stopped: `${this.config.serviceId}.event.service.stopped`,
    paused: `${this.config.serviceId}.event.service.paused`,
    processingError: `${this.config.serviceId}.event.service.processing_error`,
    commandRefused: `${this.config.serviceId}.event.service.command_refused`
  };

  private serviceResumeNatsCommand = `${this.config.serviceId}.command.service.resume`;

  constructor(
    private config: SchedulerServiceConfig,
    private connectionOptions: ConnectionOptions,
    private scheduler: IMessageProcessor
  ) {
  }

  private async connectToNatsServers(): Promise<NatsConnection> {
    try {
      console.log('Connecting to NATS servers...');
      const conn = await connect(this.connectionOptions);
      console.log(`connected to ${conn.getServer()}`);
      if (!conn) throw new Error('NATS connection is not available');
      return conn;
    } catch (err) {
      throw new Error(
        `Error connecting to ${JSON.stringify(this.connectionOptions.servers)}`
      );
    }
  }


  private async getStreamsOfInterest(): Promise<string[]> {
    const streams: string[] = [];
    let subjectsWithoutStream: string[] = [];
    for (const subject of this.scheduler.getSubjectsOfInterest()) {
      let streamName: string | null = null;
      try {
        streamName = await this.jsm!.streams.find(subject);
      } catch (err) { }

      if (streamName) {
        streams.push(streamName);
      } else {
        subjectsWithoutStream.push(subject);
      }
    }
    if (subjectsWithoutStream.length > 0) {
      if (!this.config.allowCreateServiceStream) {
        throw new Error(`Streams not found for subjects "${subjectsWithoutStream.join(', ')}"`);
      } else {
        if (streams.includes(this.config.serviceId)) {
          throw new Error(
            `Streams not found for subjects:\n${subjectsWithoutStream.map(s=>'  - '+s).join('\n')}"\n` +
            `Config allowes to automatically ctreate a stream.\n` +
            `But the stream with name ${this.config.serviceId} already exists\n`+
            `Please create a new stream with a different name manually.`);
        }

        // if all subjects start with serviceId, change with wildcard
        if (subjectsWithoutStream.every((subject) => subject.startsWith(this.config.serviceId) + '.')) {
          subjectsWithoutStream = [this.config.serviceId + '.' + '>'];
        }

        const streamConfig: Partial<StreamConfig> = {
          name: this.config.serviceId,
          subjects: subjectsWithoutStream,
        };
        await this.jsm!.streams.add(streamConfig);
        streams.push(this.config.serviceId);
      }
    }

    return streams;
  }

  private getMessageData(msg: JsMsg): unknown {
    if (!msg.data) throw new IncorrectMessageError('Message data is empty');
    let data: any;
    try {
      data = JSON.parse(msg.data.toString());
    } catch (err) {
      throw new IncorrectMessageError('Message data is not a valid JSON');
    }
    return data;
  }

  private validateMessageData(data: any) {
    const fieldsMissing = ['id', 'origin', 'timestamp'].filter((field) => !data[field]);
    if (fieldsMissing.length > 0) {
      throw new IncorrectMessageError('Message data is missing fields: ' + fieldsMissing.join(', '));
    }
  }

  async processRawMessage(msg: JsMsg) {
    const match = this.scheduler.getSubjectsOfInterest().some((subject) => msg.subject === subject);
    if (!match) {
      debugLog(` - ignored`);
      msg.ack();
      return;
    }

    let messageData;
    try {
      messageData = this.getMessageData(msg);
      this.validateMessageData(messageData);
      await this.scheduler.processMessage(msg, messageData);
      msg.ack();
      this.consequentialFailures = 0;
    } catch (err) {

      const errorMessage = {
        message: {
          subject: msg.subject,
          original: messageData,
          error: err?.toString(),
          errorJson: err instanceof IncorrectMessageError ? err.errorJson : undefined
        }
      };

      const incorrectMessage = err instanceof IncorrectMessageError;

      const errorSubject = incorrectMessage
        ? this.serviceSubjects.commandRefused
        : this.serviceSubjects.processingError;

      await this.publish(errorSubject, errorMessage);

      if (incorrectMessage) {
        // incorrect message is not counted as a failure
        msg.ack();
        debugLog('Incorrect message: ', errorMessage);
      } else {
        this.consequentialFailures++;
        if (this.consequentialFailures >= this.config.failuresLimit) {
          await this.publish(this.serviceSubjects.paused, {
            reason: 'Too many consecutive failures, check failures log.',
            resumeNatsCommand: this.serviceResumeNatsCommand,
            lastError: errorMessage
          });
          this.pause();
        }

        console.error('Error processing message:', err);
      }

    }
  }

  async runConsumer(consumer: Consumer) {

    let consumerMessages: ConsumerMessages;

    // Heartbeats recovery loop
    while (!this.terminated) {
      consumerMessages = await consumer.consume({ max_messages: 1 });

      const consumerIndex = this.consumers.indexOf(consumer);
      this.consumersMessages[consumerIndex] = consumerMessages;

      // watch the to see if the consume operation misses heartbeats
      (async () => {
        for await (const s of await consumerMessages!.status()) {
          if (s.type === ConsumerEvents.HeartbeatsMissed) {
            // you can decide how many heartbeats you are willing to miss
            const n = s.data as number;
            console.log(`${n} heartbeats missed`);
            if (n === 2) {
              // by calling `stop()` the message processing loop ends
              // in this case this is wrapped by a loop, so it attempts
              // to re-setup the consume
              consumerMessages!.stop();
            }
          }
          if (this.terminated) break;
        }
      })();

      for await (const m of consumerMessages!) {
        debugLog(`${m.seq} ${m?.subject}`);

        await this.processRawMessage(m);
        if (this.terminated) break;
      }
    }

    this.consumersStopped++;
    if (this.consumersStopped === this.consumers.length) {
      this.allConsumersStoppedCallback!();
    }
  }

  async setupConsumer(streamName: string) {
    try {
      // add a new durable consumer
      const consumerConfig: Partial<ConsumerConfig> = {
        durable_name: this.config.serviceId,
        ack_policy: AckPolicy.Explicit,
        ack_wait: 10 * 1000_000_000, // 10 seconds
      };
      await this.jsm!.consumers.add(streamName, consumerConfig);

      const consumer = await this.js!.consumers.get(streamName, consumerConfig.durable_name);
      this.consumers.push(consumer);
      this.runConsumer(consumer); // do not wait
      console.log('Consumer is ready for stream ' + streamName);

    } catch (err) {
      throw new Error('Error setting up a consumer for stream ' + streamName + ': ' + err);
    }
  }

  async setupTelemetry() {
    this.telemetrySubscription = await this.conn?.subscribe('services.>');
    (async () => {
      for await (const m of this.telemetrySubscription!) {
        try {
          const content = m.data.toString();
          console.log('Telemetry message:', m.subject, content);
          const contentObject = JSON.parse(content ?? {});
          if (m.subject === 'services.discovery.request') {
            await this.conn!.publish('services.discovery.response', JSON.stringify({ requestId: contentObject.id, serviceId: this.config.serviceId, status: 'running' }));
          }
        } catch (err) {
          console.error('Error processing telemetry message:', err);
        }

      }
    })()
  }

  async start() {
    console.log('NATS Scheduler is starting...');
    this.conn = await this.connectToNatsServers();
    this.setupTelemetry();

    try {
      this.jsm = await this.conn!.jetstreamManager();
      if (!this.jsm) throw new Error('JetStream Manager is not available');
    } catch (err) {
      throw new Error('Error connecting to JetStream Manager');
    }

    try {
      this.js = this.conn.jetstream();
      if (!this.js) throw new Error('JetStream is not available');
    } catch (err) {
      throw new Error('Error connecting to JetStream');
    }

    await this.scheduler.init(this);
    console.log('Listening for messages:');
    this.scheduler.getSubjectsOfInterest().forEach((subject) => {
      console.log('  - ' + subject);
    });

    const streamsOfInterest = await this.getStreamsOfInterest();
    for (const stream of streamsOfInterest) {
      await this.setupConsumer(stream);
    }

  }

  async notifyServiceStarted() {
    await this.publish(this.serviceSubjects.started, {});
  }

  async publish(subject: string, data: any, options?: Partial<JetStreamPublishOptions>) {
    try {
      data = data ?? {};
      data = {
        ...data,
        id: uuidv4(),
        origin: this.config.serviceId,
        timestamp: +new Date(),
      };
      await this.js!.publish(subject, JSON.stringify(data), options);
    } catch (err) {
      throw new Error('Error publishing message to NATS');
    }
  }

  // Request to stop the service and wait for all consumers to stop
  async stopConsumers() {
    await new Promise<void>((resolve) => {
      (async () => {
        this.publish(this.serviceSubjects.stopped, {});

        this.allConsumersStoppedCallback = resolve;

        this.terminated = true;
        this.consumersMessages.forEach((consumerMessages) => {
          consumerMessages.stop();
        });

      })();
    });
    this.telemetrySubscription?.unsubscribe();
  }

  async stop() {
    this.stopped = true;
    await this.stopConsumers();

    await this.conn!.drain();
    await this.conn!.close();
    console.log('NATS connection closed');

    this.terminatedCallback!();
  }

  async pause() {
    await this.stopConsumers();

    // wait for NATS command to resume
    const subscription: Subscription = this.conn!.subscribe(this.serviceResumeNatsCommand);
    const subIterator = subscription[Symbol.asyncIterator]();

    console.log('Service is paused. Waiting for resume command... '+ this.serviceResumeNatsCommand);
    await subIterator.next();

    console.log('Service is resumed. Restarting server...');

    subscription.unsubscribe();

    await this.conn!.drain();
    await this.conn!.close();
    console.log('NATS connection closed');

    this.terminatedCallback!();
  }

  async waitUntilTerminated() {
    await new Promise<void>((resolve) => {
      this.terminatedCallback = resolve;
    });
  }

}
