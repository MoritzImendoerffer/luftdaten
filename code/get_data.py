"""
Download the data
"""

import datetime
import subprocess
import time

def runcmd(cmd, verbose=False, *args, **kwargs):
    """
    Runs a command using subprocess.Popen
    :param cmd:
    :param verbose:
    :param args:
    :param kwargs:
    :return:
    """
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True
    )
    start = time.time()
    std_out, std_err = process.communicate()
    end = time.time()
    print(f'Downloaded from {cmd.split("/")[-2]} in :', (end-start), 's')
    if verbose:
        print(std_out.strip(), std_err)
    pass

url = "https://archive.sensor.community "
start_date = "2015-10-01"
start_date = "2017-08-31"

split = start_date.split('-')
start_date = datetime.datetime(year=int(split[0]),
                               month=int(split[1]),
                               day=int(split[2]))

timedelta = datetime.timedelta(days=2)

end_date = datetime.datetime.now().strftime("%Y-%m-%d")
split = end_date.split('-')
end_date = datetime.datetime(year=int(split[0]),
                             month=int(split[1]),
                             day=int(split[2]))

base_cmd = "wget -A csv,txt -r -np -nc -l1 --no-check-certificate -e robots=off -P ../data/ http://archive.sensor.community/"
base_cmd = "wget -A csv,txt -r -np -nc -l1 --no-check-certificate -e robots=off -P ../data/ http://archive.sensor.community/"

max_iter = 1000
idx = 0
while start_date != end_date:

    date_string = start_date.strftime("%Y-%m-%d")
    cmd = base_cmd + date_string + '/'
    runcmd(cmd)

    start_date += timedelta
    idx += 2
    if idx >= max_iter:
        break
    time.sleep(1)

