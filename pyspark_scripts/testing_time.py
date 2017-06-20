import os
import time
cmd1 = "cp GIT/mount_pnt2/fuse_testing/1000 ~"
cmd2 = "cp GIT/mount_pnt2/fuse_testing/10000 ~"
cmd3 = "cp GIT/mount_pnt2/fuse_testing/100000 ~"
cmd4 = "cp GIT/mount_pnt2/fuse_testing/1000000 ~"

cmds = [cmd1, cmd2, cmd3]#, cmd4]
repeat = 10
store = []
for cmd in cmds:
  temp = []
  for _ in xrange(repeat):
    start = time.time()
    os.system(cmd)
    end = time.time()
    temp.append((end-start))
    os.system("rm -f ~/1000*")
  store.append(temp)

for s in store:
  print s

