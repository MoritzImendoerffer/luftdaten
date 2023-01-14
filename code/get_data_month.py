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
start_date = "2022-06"

split = start_date.split('-')
start_date = datetime.datetime(year=int(split[0]),
                               month=int(split[1]),
                               day=int(1))

timedelta = datetime.timedelta(weeks=4)

end_date = datetime.datetime.now().strftime("%Y-%m-%d")
split = end_date.split('-')
end_date = datetime.datetime(year=int(split[0]),
                             month=int(split[1]),
                             day=int(1))


base_cmd = "wget -A '*.zip' -r -np -nc -l3 --no-check-certificate -e robots=off -P ../data/ https://archive.sensor.community/csv_per_month/"

max_iter = 1000
idx = 0
while start_date != end_date:

    date_string = start_date.strftime("%Y-%m")
    cmd = base_cmd + date_string + '/'
    runcmd(cmd)

    start_date -= timedelta
    idx += 2
    if idx >= max_iter:
        break
    time.sleep(1)

