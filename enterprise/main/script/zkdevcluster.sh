#!/bin/sh
source $(dirname $0)/bootstrap.sh

MAINCLASS=org.apache.zookeeper.server.quorum.QuorumPeerMain

$SCRIPTDIR/shutdown.sh $MAINCLASS pid/zk

mkdir -p etc pid
rm -f pid/zk

for ZK in {1,2,3}; do
    ZKCONF=etc/zk$ZK.cfg

    echo tickTime=2000 > $ZKCONF
    echo initLimit=10  >>$ZKCONF
    echo syncLimit=5   >>$ZKCONF

    echo dataDir=data/zk$ZK >>$ZKCONF
    echo clientPort=218$ZK  >>$ZKCONF

    echo server.1=localhost:2888:3888 >>$ZKCONF
    echo server.2=localhost:2889:3889 >>$ZKCONF
    echo server.3=localhost:2890:3890 >>$ZKCONF

    rm -rf data/zk$ZK
    mkdir -p data/zk$ZK
    echo $ZK > data/zk$ZK/myid

    java -cp $LIBDIR/log4j-1.2.15.jar:$LIBDIR/zookeeper-3.2.1.jar\
         $MAINCLASS $ZKCONF &
    echo $! >> pid/zk
done

