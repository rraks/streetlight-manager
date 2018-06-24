"""
Demonstrates how to use the background scheduler1 to schedule a job that executes on 3 second
intervals.
"""

from datetime import datetime
import time
import os

from apscheduler.schedulers.background import BackgroundScheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())

def tick1():
    print("BLAH")

if __name__ == '__main__':
    scheduler1 = BackgroundScheduler()
    scheduler1.add_job(tick, 'interval', seconds=3)
    scheduler1.start()
    scheduler2 = BackgroundScheduler()
    scheduler2.add_job(tick1, 'interval', seconds=2)
    scheduler2.start()

    print("Starting scheduler 2")
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler1.shutdown()
