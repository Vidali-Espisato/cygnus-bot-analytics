import os
import time
import random
import subprocess
from datetime import datetime, timedelta

files_array = []

def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        for f in files:
            f = os.path.join(root, f)
            files_array.append(f)

        for d in dirs:
            if d == ".git":
                continue
            list_files(os.path.join(root, d))

        break

list_files(".")

dates_array = []

total = 365 * 2

for i in range(total):
    the_date = datetime.now() - timedelta(total - i)

    if the_date.isoweekday() > 5:
        continue

    dates_array.append(str(the_date).split(".")[0])



subprocess.call(["sudo", "timedatectl", "set-ntp", "0"])

def set_time(time_string):
    subprocess.call(["sudo", "timedatectl", "set-time", time_string])
    s = subprocess.getstatusoutput('date -I')

    if s[1] != time_string.split(" ")[0]:
        print(s)
        time.sleep(1)
        set_time(time_string)

set_time(dates_array[0])

def init():
    subprocess.call(["git", "branch", "-M", "main"])
    subprocess.call(["git", "init"])
    subprocess.call(["git", "config", "user.name", "Manish Roy"])
    subprocess.call(["git", "config", "user.email", "manish.11612939@lpu.in"])
    subprocess.call(["git", "config", "--add", "safe.directory", os.path.abspath(".")])

init()

files_count = len(files_array)
dates_count = len(dates_array)

step = int(dates_count / files_count) + 1

pairs = []

file_idx = 0
for i in range(0, dates_count, step):
    file_path = files_array[file_idx]
    date_selected = dates_array[i]

    pairs.append((file_path, date_selected))
    file_idx += 1

    if file_idx >= files_count:
        file_idx -= 1
        break

remaining = files_array[file_idx:]
for r in remaining:
    ridx = random.randint(0, total - 1)
    the_date = datetime.now() - timedelta(total - ridx)

    pairs.append((r, str(the_date).split(".")[0]))

print(len(pairs))

for f, d in pairs:
    set_time(d)
    subprocess.call(["git", "add", f])
    subprocess.call(["git", "commit", "-m", f"added { f.split('/')[-1] }"])

subprocess.call(["sudo", "timedatectl", "set-ntp", "1"])
time.sleep(1)