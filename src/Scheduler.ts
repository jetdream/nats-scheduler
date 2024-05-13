import cronParser from 'cron-parser';
import {
  IEventPublisher,
  IMessageProcessor,
  IncorrectMessageError,
} from './types';

import {AsyncDatabase} from 'promised-sqlite3';
import {JetStreamPublishOptions, JsMsg} from 'nats';
import {debugLog} from './utils';

export type SchedulerConfig = {
  serviceId: string;
  databasePath: string;
};

export interface SchedulerCommandOneOffSchedule {
  recurring: false;
  subscriptionId: string;
  scheduleAt: number;
  notify: {
    subject: string;
    message: any;
  };
}

const validateCommandOneOffSchedule = (msg: SchedulerCommandOneOffSchedule) => {
  const valid =
    typeof msg.recurring === 'boolean' &&
    typeof msg.subscriptionId === 'string' &&
    typeof msg.scheduleAt === 'number' &&
    typeof msg.notify === 'object' &&
    typeof msg.notify.subject === 'string' &&
    typeof msg.notify.message === 'object';
  if (!valid) {
    throw new IncorrectMessageError('Incorrect message structure');
  }
};

export interface SchedulerCommandRecurringSchedule {
  recurring: true;
  subscriptionId: string;
  cronPattern: string;
  timezone: string;
  notify: {
    subject: string;
    message: any;
  };
}

const validateCommandRecurringSchedule = (
  msg: SchedulerCommandRecurringSchedule
) => {
  const valid =
    typeof msg.recurring === 'boolean' &&
    typeof msg.subscriptionId === 'string' &&
    typeof msg.cronPattern === 'string' &&
    typeof msg.timezone === 'string' &&
    typeof msg.notify === 'object' &&
    typeof msg.notify.subject === 'string' &&
    typeof msg.notify.message === 'object';
  if (!valid) {
    throw new IncorrectMessageError('Incorrect message structure');
  }
};

export interface SchedulerCommandCancel {
  subscriptionId: string;
}

const validateCommandCancel = (msg: SchedulerCommandCancel) => {
  const valid = typeof msg.subscriptionId === 'string';
  if (!valid) {
    throw new IncorrectMessageError('Incorrect message structure');
  }
};

export type SchedulerCommand =
  | SchedulerCommandOneOffSchedule
  | SchedulerCommandRecurringSchedule
  | SchedulerCommandCancel;

type EventRecord = {
  id: number;
  subscriptionId: string;
  executeAt: string;
  cronPattern: string;
  timezone: string;
  notifyData: string;
  executed: boolean;
  recurring: boolean;
};

export class Scheduler implements IMessageProcessor {
  private publisher!: IEventPublisher;
  private db!: AsyncDatabase;
  private nextEventTimer: NodeJS.Timeout | null = null;

  constructor(private config: SchedulerConfig) {}

  async init(publisher: IEventPublisher): Promise<void> {
    this.publisher = publisher;

    await this.setupDB();
    await this.setNextEventTimer();
  }

  getSubjectsOfInterest(): string[] {
    return [
      `${this.config.serviceId}.command.scheduler.schedule`,
      `${this.config.serviceId}.command.scheduler.cancel`,
    ];
  }

  async processMessage(message: JsMsg, data: unknown): Promise<void> {
    const subject = message.subject;
    const payload = data as SchedulerCommand;

    // TODO: validate payload

    switch (subject) {
      case `${this.config.serviceId}.command.scheduler.schedule`:
        if ((payload as SchedulerCommandRecurringSchedule).recurring) {
          validateCommandRecurringSchedule(
            data as SchedulerCommandRecurringSchedule
          );
          await this.scheduleRecurringEvent(
            data as SchedulerCommandRecurringSchedule
          );
        } else {
          validateCommandOneOffSchedule(data as SchedulerCommandOneOffSchedule);
          await this.scheduleOneOffEvent(
            data as SchedulerCommandOneOffSchedule
          );
        }
        break;
      case `${this.config.serviceId}.command.scheduler.cancel`:
        validateCommandCancel(data as SchedulerCommandCancel);
        await this.removeEvent(payload.subscriptionId);
        break;
    }
  }

  private async setupDB(): Promise<void> {
    this.db = await AsyncDatabase.open(
      this.config.databasePath + '/scheduler.db'
    );

    await this.db.run(
      'CREATE TABLE IF NOT EXISTS events ' +
        '(id INTEGER PRIMARY KEY AUTOINCREMENT, subscriptionId TEXT, executeAt DATETIME, cronPattern TEXT, ' +
        'timezone TEXT, notifyData TEXT, executed BOOLEAN, recurring BOOLEAN)'
    );
    await this.db.run(
      'CREATE INDEX IF NOT EXISTS idx_executeAt ON events (executeAt)'
    );
    await this.db.run(
      'CREATE INDEX IF NOT EXISTS idx_subscriptionId ON events (subscriptionId)'
    );
  }

  private async makeSureSubscriptionIdIsUnique(subscriptionId: string) {
    const existing = await this.db.get(
      'SELECT * FROM events WHERE subscriptionId = ?',
      [subscriptionId]
    );
    if (existing) {
      throw new IncorrectMessageError(
        'Subscription with this ID already exists: ' + subscriptionId
      );
    }
  }

  private async scheduleOneOffEvent(
    cmd: SchedulerCommandOneOffSchedule
  ): Promise<void> {
    await this.makeSureSubscriptionIdIsUnique(cmd.subscriptionId);

    const query =
      'INSERT INTO events (subscriptionId, executeAt, notifyData, executed, recurring) VALUES (?, ?, ?, ?, ?)';
    await this.db.run(query, [
      cmd.subscriptionId,
      new Date(cmd.scheduleAt).toISOString(),
      JSON.stringify(cmd.notify),
      0,
      0,
    ]);

    debugLog(`One-off event scheduled with ID: ${cmd.subscriptionId}`);
    await this.setNextEventTimer();
  }

  private async scheduleRecurringEvent(
    cmd: SchedulerCommandRecurringSchedule
  ): Promise<void> {
    await this.makeSureSubscriptionIdIsUnique(cmd.subscriptionId);

    let nextTime: Date;
    try {
      const interval = cronParser.parseExpression(cmd.cronPattern, {
        tz: cmd.timezone,
      });
      nextTime = interval.next().toDate();
    } catch (err) {
      throw new IncorrectMessageError('Invalid cron pattern. ' + err);
    }

    const query =
      'INSERT INTO events (subscriptionId, executeAt, cronPattern, timezone, notifyData, executed, recurring) VALUES (?, ?, ?, ?, ?, ?, ?)';
    await this.db.run(query, [
      cmd.subscriptionId,
      nextTime.toISOString(),
      cmd.cronPattern,
      cmd.timezone,
      JSON.stringify(cmd.notify),
      0,
      1,
    ]);

    debugLog(`Recurring event scheduled with ID: ${cmd.subscriptionId}`);
    await this.setNextEventTimer();
  }

  private async setNextEventTimer(extraDelayMs = 0): Promise<void> {
    if (this.nextEventTimer) {
      clearTimeout(this.nextEventTimer);
    }

    try {
      if (
        process.env.NODE_ENV === 'development' ||
        process.env.NODE_ENV === 'test'
      ) {
        // request number of active subscriptions
        const activeSubscriptions: any = await this.db.get(
          'SELECT COUNT(*) as cnt FROM events WHERE executed = 0'
        );
        debugLog(`Active subscriptions: ${activeSubscriptions['cnt']}`);
      }

      const row = (await this.db.get(
        'SELECT * FROM events WHERE executed = 0 ORDER BY executeAt ASC LIMIT 1',
        []
      )) as EventRecord;

      if (row) {
        const now = new Date();
        const delay = Math.max(
          extraDelayMs,
          new Date(row.executeAt).getTime() - now.getTime() + extraDelayMs
        );
        if (delay > 0) {
          debugLog(`Next event in ${delay} ms`);
          this.nextEventTimer = setTimeout(() => {
            this.triggerEvent(row);
          }, delay);
        } else {
          await this.triggerEvent(row);
        }
      }
    } catch (err) {
      // TODO handle
      console.error(err);
    }
  }

  private async triggerEvent(event: EventRecord): Promise<void> {
    try {
      const notifyData = JSON.parse(event.notifyData);
      const message = {
        ...notifyData.message,
        subscriptionId: event.subscriptionId,
        scheduledFor: +new Date(event.executeAt),
      };
      const options: Partial<JetStreamPublishOptions> = {
        // this will prevent duplicate messages even on distributed systems
        msgID: `${event.subscriptionId}:${event.executeAt}`,
      };
      await this.publisher.publish(notifyData.subject, message, options);
    } catch (err) {
      console.error(`Error triggering event: ${err}`);
      console.error('Delaying next attempt for 5 seconds.');
      await this.setNextEventTimer(5000);
    }

    debugLog(`Event triggered for ${event.subscriptionId}`);
    if (event.recurring) {
      const nextTime = cronParser
        .parseExpression(event.cronPattern, {tz: event.timezone})
        .next()
        .toDate();
      await this.db.run(
        'UPDATE events SET executeAt = ?, executed = 0 WHERE id = ?',
        [nextTime.toISOString(), event.id]
      );
      await this.setNextEventTimer();
    } else {
      this.removeEvent(event.subscriptionId);
    }
  }

  private async removeEvent(subscriptionId: string): Promise<void> {
    await this.db.run('DELETE FROM events WHERE subscriptionId = ?', [
      subscriptionId,
    ]);

    debugLog(`Removed event with Subscription ID: ${subscriptionId}`);
    await this.setNextEventTimer();
  }
}
