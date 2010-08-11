package org.neo4j.kernel.impl.ha;

public interface Broker
{
    Master getMaster();
    
    void setLastCommittedTxId( long txId );
    
    boolean thisIsMaster();
}
