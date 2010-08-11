package org.neo4j.kernel.impl.ha;

public interface Broker
{
    Master getMaster();
    
    SlaveContext getSlaveContext();
    
    void setLastCommittedTxId( long txId );
    
    boolean thisIsMaster();
}
