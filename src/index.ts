/* eslint-disable no-constant-condition */
/* eslint-disable no-process-exit */
/* eslint-disable quotes */
/* eslint-disable prettier/prettier */
require('dotenv').config();
import { ConnectionOptions } from 'nats';
import { SchedulerService, SchedulerServiceConfig } from './SchedulerService';

import { envBool, envInt, envStr } from './utils';
import { Scheduler, SchedulerConfig } from './Scheduler';

// Service parameters
const SERVICE_ID = envStr('SERVICE_ID', 'will be used to identify the service in the NATS server and origin of the messages');
const ALLOW_CREATE_SERVICE_STREAM = envBool('ALLOW_CREATE_SERVICE_STREAM', 'Allowed to create service stream');
const SERVICE_FAILURES_LIMIT = envInt('SERVICE_FAILURES_LIMIT', 'Service failures limit');
const NATS_SERVERS_CONFIG = envStr('NATS_SERVERS_CONFIG', 'NATS servers configuration');
const DATABASE_PATH = envStr('DATABASE_PATH', 'Path to the database file');

async function main() {
  while (true) {



    const schedulerConfig: SchedulerConfig = {
      serviceId: SERVICE_ID,
      databasePath: DATABASE_PATH
    }
    const scheduler = new Scheduler(schedulerConfig);

    const connectionOptions: ConnectionOptions = {
      name: SERVICE_ID,
      servers: NATS_SERVERS_CONFIG?.split(',') || [],
    };

    const serviceConfig: SchedulerServiceConfig = {
      failuresLimit: SERVICE_FAILURES_LIMIT,
      serviceId: SERVICE_ID,
      allowCreateServiceStream: ALLOW_CREATE_SERVICE_STREAM
    }

    const schedulerService = new SchedulerService(serviceConfig, connectionOptions, scheduler);
    await schedulerService.start();


    await schedulerService.notifyServiceStarted();

    process.removeAllListeners('SIGINT');
    process.on('SIGINT', async () => {
      // wait for 30 seconds before exiting forcefully
      setTimeout(() => {
        console.log('Exiting forcefully');
        process.exit(1);
      }, 30000);

      console.log('Received SIGINT');
      await schedulerService.stop();
      process.exit();
    });

    console.log('Everything is up and running');
    await schedulerService.waitUntilTerminated();

    if (schedulerService.stopped) break;
  }

}

main();
