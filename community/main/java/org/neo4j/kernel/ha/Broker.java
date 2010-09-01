package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;

public interface Broker
{
    void invalidateMaster();
    
    Master getMaster();
    
    void setLastCommittedTxId( long txId );
    
    boolean thisIsMaster();
    
    int getMyMachineId();
    
    // I know... this isn't supposed to be here
    Object instantiateMasterServer( GraphDatabaseService graphDb );
    
    void shutdown();
}
