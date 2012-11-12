
from shutil import copyfile
import os,sys

POMFILE = 'pom.xml'
POMFILE_BACKUP = 'pom.xml.releasebackup'
VERSION_MARKER = '<!--RV:-->'

# Note: We use os.system instead of subprocess
# because otherwise we need to determine maven and
# git exec paths on windows.
BUILD_COMMAND = "mvn clean install -Ppypi"

def set_project_version(v, in_path, out_path):
  with open(in_path,'r') as original_pom:
    with  open(out_path,'w') as new_pom:
      for line in original_pom:
        vm = line.find(VERSION_MARKER)
        if vm >= 0:
          start = vm + len(VERSION_MARKER)
          stop = line.find('<', start)
          line = "".join([line[0:start], v, line[stop:]])
        
        new_pom.write(line)
          

def get_project_version(in_path):
  with open(in_path,'r') as pom:
    for line in pom:
      vm = line.find(VERSION_MARKER)
      if vm >= 0:
        start = vm + len(VERSION_MARKER)
        stop = line.find('<', start)
        return line[start:stop]

if __name__ == '__main__':
  try:
    os.remove(POMFILE_BACKUP)
  except: 
    pass
  copyfile(POMFILE, POMFILE_BACKUP)
  
  print "Preparing release.."
  print "Current project version is %s." % get_project_version(POMFILE_BACKUP)
  
  release_version = raw_input("What should the release version be? ")
  new_dev_version = raw_input("What should the new version be after release? ")
  
  print
  print "Release version: %s" % release_version
  print "New dev version: %s" % new_dev_version
  ok = raw_input("Confirm (y/N):")

  if ok.lower() != 'y':
    print "Aborting."
    sys.exit(0)

  print "Setting new project version.."
  os.remove(POMFILE)
  set_project_version(release_version, POMFILE_BACKUP, POMFILE)
  
  print "Running build.."
  print BUILD_COMMAND
  result = os.system(BUILD_COMMAND)

  if result != 0:
    print
    print "Build command failed :("
    print "Restoring pom from backup file.."
    os.remove(POMFILE)
    copyfile(POMFILE_BACKUP, POMFILE)
    print "Exiting, feeling sadness."
    sys.exit(-1)
    
  print 
  print "Build done!"
  print "Creating scm tag.."
  os.system('git commit -am "Release '+release_version+'"')
  os.system('git tag -a '+release_version+' -m "Release "'+release_version+'"')
  print
  print "Done, tag is in local repo."
  
  print 
  print "Switching back to development version"
  os.remove(POMFILE)
  set_project_version(new_dev_version, POMFILE_BACKUP, POMFILE)
  os.remove(POMFILE_BACKUP)
  
  print
  print "Committing changes.."
  os.system('git commit -am "Back to development version."')
  
  
