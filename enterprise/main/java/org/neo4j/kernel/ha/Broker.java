package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.Machine;

public interface Broker
{
    Pair<Master, Machine> getMaster();
    
    Machine getMasterExceptMyself();
    
    void setLastCommittedTxId( long txId );
    
    boolean iAmMaster();
    
    int getMyMachineId();
    
    // I know... this isn't supposed to be here
    Object instantiateMasterServer( GraphDatabaseService graphDb );
    
    void rebindMaster();
    
    void shutdown();
}
