import os
import time
import subprocess
import sys
import urllib2

url1 = "http://10.239.227.6:8010/FileSpray/DownloadFile?Name=1000&NetAddress=10.239.227.6&Path=%2Fvar%2Flib%2FHPCCSystems%2Fmydropzone&OS=2"
url2 = "http://10.239.227.6:8010/FileSpray/DownloadFile?Name=10000&NetAddress=10.239.227.6&Path=%2Fvar%2Flib%2FHPCCSystems%2Fmydropzone&OS=2"
url3 = "http://10.239.227.6:8010/FileSpray/DownloadFile?Name=100000&NetAddress=10.239.227.6&Path=%2Fvar%2Flib%2FHPCCSystems%2Fmydropzone&OS=2"
url4 = "http://10.239.227.6:8010/FileSpray/DownloadFile?Name=1000000&NetAddress=10.239.227.6&Path=%2Fvar%2Flib%2FHPCCSystems%2Fmydropzone&OS=2"
url5 = "http://10.239.227.6:8010/FileSpray/DownloadFile?Name=10000000&NetAddress=10.239.227.6&Path=%2Fvar%2Flib%2FHPCCSystems%2Fmydropzone&OS=2"
url6 = "http://10.239.227.6:8010/FileSpray/DownloadFile?Name=100000000&NetAddress=10.239.227.6&Path=%2Fvar%2Flib%2FHPCCSystems%2Fmydropzone&OS=2"

urls = [url1, url2, url3, url4, url5, url6]
repeat = 10

all = []
for url in urls:
    temp = []
    for _ in xrange(repeat):
        print ". ",
        start = time.time()
        import wget
        file_name = wget.download(url)
        end = time.time()
        temp.append(end-start)
        os.system('rm -f ' + file_name)
        sys.stdout.flush()
    all.append(temp)
    print
    print temp

for al in all:
    print al