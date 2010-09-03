package org.neo4j.kernel.ha;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.Machine;

public interface ResponseReceiver
{
    SlaveContext getSlaveContext( int eventIdentifier );
    
    <T> T receive( Response<T> response );
    
    void newMaster( Pair<Master, Machine> master, Exception e );
}
