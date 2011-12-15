#!/bin/bash

cd /tmp
wget -O java.exe http://javadl.sun.com/webapps/download/AutoDL?BundleId=57240
chmod +x java.exe
/tmp/java.exe /s
rm java.exe


