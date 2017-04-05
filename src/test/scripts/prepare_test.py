#!/usr/bin/env python
import sys
import shutil
import glob
from filecmp import cmp
import subprocess
from zipfile import ZipFile
import os
import tempfile

if len(sys.argv) != 4:
    print "Wrong number of arguments"
    sys.exit(1)

propFile = sys.argv[1]
home = sys.argv[2]
icaturl = sys.argv[3]

with tempfile.NamedTemporaryFile(delete=False) as f:
    name = f.name
shutil.copy(propFile, name)
with open (name, "a") as f:
    f.write("icat.url = " + icaturl)

if os.path.exists('src/test/install/run.properties') and cmp(name, 'src/test/install/run.properties'):
    sys.exit(0)

print "Installing with " + propFile

shutil.copy(name, 'src/test/install/run.properties')
os.remove(name)

with open('src/test/install/setup.properties', 'w') as f:
    f.write('secure         = true\n')
    f.write('container      = Glassfish\n')
    f.write('home           = ' + home + '\n')
    f.write('port           = 4848\n')
    f.write('libraries=ids.storage_test*.jar')

with open("src/test/install/run.properties.example", "w") as f:
    pass

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



