import sys
import os
import time


def copy_file(source, destination):
    line_no =0
    s = open(source, 'r')
    d = open(destination, 'w')
    for line in s:
        d.write(line)
        if line_no % 1000 == 0:
            d.flush()
        line_no += 1
    s.close()
    d.close()

if __name__ == "__main__":
    # if len(sys.argv) > 3: exit()
    # source = sys.argv[1]
    # destination = sys.argv[2]
    # copy_file(source, destination)
    for _ in xrange(1):
        start_time = time.time()
        copy_file("/home/osboxes/GIT/mount_pnt2/fuse_testing/100000000", "/home/osboxes/100000000")
        os.system('rm -f /home/osboxes/100000000')
        print time.time() - start_time