import time

date_time = '2:02'
pattern = '%H:%M'
epoch = int(time.mktime(time.strptime(date_time, pattern)))
print (epoch)

