#!/root/.pyenv/shims/python
# sunchao
# 2017 08 18

import time
import sys
import os
import json
import signal
import subprocess
from urllib import request

if len(sys.argv) == 7:
    script, local_addr, tcurl, host, app, name, ori_name = sys.argv
    transcode_args = None
else:
    script, local_addr, tcurl, app, name, *args = sys.argv
    transcode_args = args.copy()

access_key = "streamingkwmanage"
log_file = '/cache/logs/err_log/ffmpeg_for_fmd.log'
ffmpeg_path = '/usr/local/sbin/ffmpeg -re -i '
ffprobe_path = '/usr/local/sbin/ffprobe -i '
PID = os.getpid()
PPID = os.getppid()
# CTL innerDNS 122.228.199.86 122.228.199.82 122.226.213.144 222.186.136.34
INNERDNS = "122.228.199.82"

class Stream():
    def __init__(self,host,app,name,tcurl):
        self.ffprobe = "{0}\"rtmp://{1}/{2}/{3} ".format(ffprobe_path,host,app,name) \
            + "tcurl={0}\" -show_streams -print_format json 2> /dev/null".format(tcurl)
        self.info = os.popen(self.ffprobe).read()
        self.v = []

    def video(self):
        ret = json.loads(self.info)["streams"]
        for i in range(0,len(ret)):
            if ret[i]["codec_type"] == "video":
                try:
                    if ret[i]["bit_rate"] and ret[i]["bit_rate"] != "0":
                        self.v.append(int(ret[i]["bit_rate"]))
                    else:
                        self.v.append(float("inf"))
                except:
                    self.v.append(float("inf"))
                if ret[i]["avg_frame_rate"]:
                    self.v.append(int(ret[i]["avg_frame_rate"].split('/')[0]))
                else:
                    self.v.append(float("inf"))
                if ret[i]["coded_width"]:
                    self.v.append(int(ret[i]["coded_width"]))
                else:
                    self.v.append(float("inf"))
        if len(self.v) != 0:
            return True
        else:
            return False

def md5_sum(string):
    import hashlib
    md5_str = hashlib.md5()
    md5_str.update(string.encode('utf-8'))
    return md5_str.hexdigest()

def sleep_for_ever():
    while True:
        time.sleep(300)
        os.kill(PPID,0)

def log_format(info,tcurl=tcurl,name=name):
    info = str(info)
    local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if transcode_args is None:
        ret = "PULL {0} {1} {2} {3} {4}\n".format(local_time,PID,tcurl,name,info)
    else:
        ret = "PUSH {0} {1} {2} {3} {4} {5}\n".format(local_time,PID,tcurl,name,transcode_args,info)
    with open(log_file, 'a') as f:
        f.write(ret)

def dns_client(domain,innerdns):
    cmd = "dig @{0} {1} +short +time=1 | grep '^[0-9]'".format(innerdns,domain)
    try:
        dns_ret = subprocess.check_output(cmd, shell=True)
    except:
        return domain
    dns_ret = dns_ret.decode('utf-8').split()
    return dns_ret

def on_play(name,app,host,local_addr,access_key):
    timestamp = int(time.time())
    access_md5 = md5_sum(str(timestamp) + name + access_key)
    dns_ret = dns_client('stream-api.devops.fastweb.com.cn',INNERDNS)
    if isinstance(dns_ret,list):
        addr = dns_ret[0]
    else:
        addr = dns_ret
    url = "http://{0}/media_dispatcher?".format(addr) \
        + "call=on_play_dispatch_rtmp&stream={0}&".format(name) \
        + "application={0}&domain={1}&".format(app,host) \
        + "node={0}&timestamp={1}&".format(local_addr,timestamp) \
        + "md5={0}".format(access_md5)
    try:
        response = request.urlopen(url)
    except:
        log_format("Dispatcher is't 200 {0}".format(url))
        exit()
    date = response.read().decode("utf-8")
    return date

def stream_map(name, ori_name):
    stream_bitrate = name.lstrip(ori_name + "_")
    transcode_info = {
        "500":("500k", "800x450", 24),
        "800":("800k", "960X540", 24),
        "1200":("1200k", "1280X720", 30),
        "2000":("2000k", "1920X1080", 30),
        "3000":("3000k", "2560X1440", "")
    }
    return transcode_info.get(stream_bitrate)

def pop_key(new_list,x):
    for i in range(0,x):
        new_list.pop(0)
    return new_list

if transcode_args is None:
    if ori_name == "":
        sleep_for_ever()
        exit()

    ret = on_play(ori_name,app,host,local_addr,access_key)
    try:
        ret = json.loads(ret)
    except:
        log_format("JSONDecodeError mybe not a json string")
        exit()
    try:
        ffmpeg_input = "\"rtmp://{0}/{1}/{2} ".format(ret["node"],app,ori_name) \
        + "tcurl={0}\" ".format(tcurl)
    except:
        log_format("KeyError {0}".format(ret))
        exit()

    ret = stream_map(name,ori_name)
    if ret is None:
        ffmpeg_args = "-c copy "
    elif ret[2] == "":
        ffmpeg_args = "-acodec libfaac -vcodec libx264 -b:v %s -s %s %s" % ret
    else:
        ffmpeg_args = "-acodec libfaac -vcodec libx264 -b:v %s -s %s -r %d " % ret

    ffmpeg_output = "-f flv \"rtmp://{0}/{1}/{2} ".format(local_addr,app,name) \
        + "tcurl={0}\"".format(tcurl)

    ffmpeg_path = ffmpeg_path + ffmpeg_input + ffmpeg_args + ffmpeg_output
else:
    if len(args)%4 != 0:
        log_format("Transcode args incomplete")
        exit()

    streaming = Stream(local_addr,app,name,tcurl)
    has_video = streaming.video()
    ffmpeg_input = "\"rtmp://{0}/{1}/{2} ".format(local_addr,app,name) \
        + "tcurl={0}\" ".format(tcurl)

    ffmpeg_path = ffmpeg_path + ffmpeg_input
    if has_video:
        while args:
            if int(args[0])*1000 >= streaming.v[0]:
                ffmpeg_args = "-map 0 -c copy "
            else:
                if int(args[1]) == -1:
                    args[1] = "30"

                if int(args[1]) > streaming.v[1]:
                    args[1] = str(streaming.v[1])

                if args[2]:
                    if int(args[2].split("x")[0]) > streaming.v[2]:
                        args[2] = str(streaming.v[2])
                    else:
                        args[2] = args[2].split("x")[0]
                else:
                    args[2] = str(streaming.v[2])

                ffmpeg_args = "-map 0 -acodec libfaac -vcodec libx264 " \
                    + "-b:v {0}k -r {1} ".format(args[0], args[1]) \
                    + "-vf scale=\"{0}:trunc(ow/a/2)*2\" ".format(args[2])

            ffmpeg_output = "-f flv \"rtmp://{0}:1835/{1}/{2} ".format(local_addr,app,args[3]) \
                + "tcurl={0}\" ".format(tcurl)
            args = pop_key(args,4)
            ffmpeg_path = ffmpeg_path + ffmpeg_args + ffmpeg_output
    else:
        ffmpeg_args = "-map 0 -c copy "
        while args:
            ffmpeg_output = "-f flv \"rtmp://{0}:1835/{1}/{2} ".format(local_addr,app,args[3]) \
                + "tcurl={0}\" ".format(tcurl)
            args = pop_key(args,4)
            ffmpeg_path = ffmpeg_path + ffmpeg_args + ffmpeg_output

log_format("ffmpeg start to run")
CHAILD = subprocess.Popen(ffmpeg_path, shell=True)

def kill_ffmpeg(SIG,stack):
    os.kill(CHAILD.pid,signal.SIGKILL)
    log_format("FMD kill the ffmpeg {0}".format(CHAILD.pid))

signal.signal(signal.SIGTERM,kill_ffmpeg)
CHAILD.wait()
log_format("ffmpeg stoped with code {0}".format(CHAILD.returncode))
