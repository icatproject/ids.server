#!/usr/bin/env python
import sys
import shutil
import glob
from filecmp import cmp
import subprocess
from zipfile import ZipFile

if len(sys.argv) != 3:
    print "Wrong number of arguments"
    sys.exit(1)

propFile = sys.argv[1]
home = sys.argv[2]

if cmp(propFile, 'src/test/install/run.properties'):
    sys.exit(0)

print "Installing with " + propFile
shutil.copy(propFile, 'src/test/install/run.properties')

f = open('src/test/install/setup.properties', 'w')
f.write('secure         = true\n')
f.write('container      = Glassfish\n')
f.write('home           = ' + home + '\n')
f.write('port           = 4848\n')
f.write('libraries=ids.storage_test*.jar')
f.close()

f = open("src/test/install/run.properties.example", "w")
f.close()

shutil.copy(glob.glob('target/ids.server-*.war')[0] , 'src/test/install/')
shutil.copy('src/main/scripts/setup', 'src/test/install/')
z = ZipFile(glob.glob("target/ids.server-*-distro.zip")[0])
bytes = z.read('ids.server/setup_utils.py')
f = open('src/test/install/setup_utils.py', 'w')
f.write(bytes)
f.close()
z.close()

p = subprocess.Popen(['./setup', "install"], cwd='src/test/install')
p.wait()



