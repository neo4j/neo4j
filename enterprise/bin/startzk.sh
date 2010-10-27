#!/bin/bash

java -cp lib/zookeeper-3.2.1.jar:lib/log4j-1.2.15.jar org.apache.zookeeper.server.quorum.QuorumPeerMain etc/zkserver.cfg
