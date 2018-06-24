"""
This example demonstrates the use of the MongoDB job store.
On each run, it adds a new alarm that fires after ten seconds.
You can exit the program, restart it and observe that any previous alarms that have not fired yet
are still active. Running the example with the --clear switch will remove any existing alarms.
"""

from datetime import datetime, timedelta
import sys
import os
import time
from apscheduler.schedulers.blocking import BlockingScheduler


def alarm(time,name):
    print('This alarm was scheduled at %s.' % name )

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_jobstore('mongodb', collection='example_jobs')
    if len(sys.argv) > 1 and sys.argv[1] == '--clear':
        scheduler.remove_all_jobs()

    alarm_time = datetime.now() + timedelta(seconds=2)
    scheduler.add_job(alarm, 'cron',second='*/3', args=[str(datetime.now()),"Hello"]) # Triggers at 3 seconds from start time and same time everyday thereafter
    print('To clear the alarms, run this example with the --clear argument.')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
        print("Started")
        while(True):
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass
