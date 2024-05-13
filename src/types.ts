import {JetStreamPublishOptions, JsMsg} from 'nats';

/**
 * Interface for event publishers.
 */
export interface IEventPublisher {
  publish(
    subject: string,
    data: any,
    options?: Partial<JetStreamPublishOptions>
  ): Promise<void>;
}

export interface IMessageProcessor {
  getSubjectsOfInterest(): string[];
  processMessage(message: JsMsg, data: unknown): Promise<void>;
  init(publisher: IEventPublisher): Promise<void>;
}

export class IncorrectMessageError extends Error {
  errorJson?: any;

  constructor(details?: string, json?: any) {
    super('Incorrect message structure. ' + details);
    this.errorJson = json;
  }
}


