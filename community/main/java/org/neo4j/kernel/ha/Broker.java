package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.ha.zookeeper.Machine;

public interface Broker
{
    void invalidateMaster();
    
    Master getMaster();
    
    Machine getMasterMachine();
    
    void setLastCommittedTxId( long txId );
    
    boolean thisIsMaster();
    
    int getMyMachineId();
    
    // I know... this isn't supposed to be here
    Object instantiateMasterServer( GraphDatabaseService graphDb );
    
    void shutdown();
}
