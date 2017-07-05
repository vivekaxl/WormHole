import os
import time
import subprocess
import sys
from copy_file import copy_file

fuse_working_dir = "/home/osboxes/GIT/python-fuse-sample/"
cmd1 = "copy_file(\"/home/osboxes/GIT/mount_pnt2/fuse_testing/1000\", \"/home/osboxes/1000\")"
cmd2 = "copy_file(\"/home/osboxes/GIT/mount_pnt2/fuse_testing/10000\", \"/home/osboxes/10000\")"
cmd3 = "copy_file(\"/home/osboxes/GIT/mount_pnt2/fuse_testing/100000\", \"/home/osboxes/100000\")"
cmd4 = "copy_file(\"/home/osboxes/GIT/mount_pnt2/fuse_testing/1000000\", \"/home/osboxes/1000000\")"
# cmd5 = "cp /home/osboxes/GIT/mount_pnt2/fuse_testing/10000000 /home/osboxes/"
# cmd5 = "cp /home/osboxes/GIT/mount_pnt2/fuse_testing/10000000_id /home/osboxes/"
cmd5 = "copy_file.py /home/osboxes/GIT/mount_pnt2/fuse_testing/10000000_id /home/osboxes/10000000_id"
cmd6 = "copy_file.py /home/osboxes/GIT/mount_pnt2/fuse_testing/100000000 /home/osboxes/"

fuse_cmd = ['python', 'passthrough_hpcc.py', '10.239.227.6', '8010', '/home/osboxes/GIT/mount_pnt2/']
cmds = [cmd1, cmd2, cmd3, cmd4]#
repeat = 10
store = []
os.chdir(fuse_working_dir)
for cmd in cmds:
  temp = []
  for _ in xrange(repeat):

    try:
      proc = subprocess.Popen(fuse_cmd, stdout=subprocess.PIPE, shell=False)
    except subprocess.CalledProcessError as e:
      print "Error: ", e.output
    time.sleep(2)
    start = time.time()
    # copy_file("/home/osboxes/GIT/mount_pnt2/fuse_testing/10000000_id", "/home/osboxes/10000000_id")
    exec(cmd)
    end = time.time()
    os.system('kill -9 ' + str(proc.pid))
    os.system("sudo umount /home/osboxes/GIT/mount_pnt2")
    temp.append((end-start))
    os.system("rm -f /home/osboxes/1000*")
    print ">>> " * 10, temp[-1]
  store.append(temp)

for s in store:
  print s

