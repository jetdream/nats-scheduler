/* eslint-disable prettier/prettier */
// generated https://chatwithgpt.bpms.dev/chat/8d2de08b-f3bb-4deb-b043-874e3c16bcee

export class RateLimiter {
  private tokens: number;
  private readonly maxTokens: number;
  private readonly refillRate: number;
  private readonly refillInterval: number;
  private queue: Array<() => void>;
  private lastRefillTime: number;

  constructor(limit: number, interval: number) {
    this.tokens = this.maxTokens = limit;
    this.refillRate = interval * 1000; // convert seconds to milliseconds
    this.refillInterval = 100; // smaller interval for more frequent checks
    this.queue = [];
    this.lastRefillTime = Date.now();

    setInterval(() => this.refillTokens(), this.refillInterval);
  }

  private refillTokens() {
    const now = Date.now();
    const elapsedTime = now - this.lastRefillTime;
    if (elapsedTime > this.refillRate) {
      const tokensToAdd =
        Math.floor(elapsedTime / this.refillRate) * this.maxTokens;
      this.tokens = Math.min(this.tokens + tokensToAdd, this.maxTokens);
      this.lastRefillTime = now;
      this.processQueue();
    }
  }

  private processQueue() {
    while (this.queue.length > 0 && this.tokens > 0) {
      const task = this.queue.shift();
      if (task) {
        task();
        this.tokens--;
      }
    }
  }

  public async limit(): Promise<void> {
    if (this.tokens > 0) {
      this.tokens--;
      return Promise.resolve();
    }

    return new Promise<void>(resolve => {
      this.queue.push(resolve);
    });
  }
}

export const env = (envName: string, description: string, type: 'boolean' | 'string' | 'number' = 'string', required = true): boolean | string | number | undefined => {
  const value = process.env[envName] as string | undefined;
  if (required && !value) {
    console.error(`Missing required environment variable: ${envName} - ${description}`);
    process.exit(1);
  }
  if (type === 'boolean') {
    switch (value?.toLowerCase()) {
      case 'true':
      case 'yes':
      case '1':
        return true;
      case 'false':
      case 'no':
      case '0':
        return false;
      default:
        console.error(`Invalid boolean value for environment variable: ${envName} - ${description}`);
        process.exit(1);
    }
  }
  if (type === 'number') {
    const parsedValue = parseInt(value!);
    if (isNaN(parsedValue)) {
      console.error(`Invalid number value for environment variable: ${envName} - ${description}`);
      process.exit(1);
    }
    return parsedValue;
  }
  return value;
}

export const envInt = (envName: string, description: string): number => {
  return env(envName, description, 'number', true) as number;
}

export const envStr = (envName: string, description: string): string => {
  return env(envName, description, 'string', true) as string;
}

export const envBool = (envName: string, description: string): boolean => {
  return env(envName, description, 'boolean', true) as boolean;
}

export function debugLog(...args: any[]) {
  if (process.env.NOdE_ENV === 'development') {
    console.log(...args);
  }
}
