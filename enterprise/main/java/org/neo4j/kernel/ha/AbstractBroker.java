package org.neo4j.kernel.ha;

import java.util.Map;

public abstract class AbstractBroker implements Broker
{
    private final int myMachineId;

    public AbstractBroker( int myMachineId )
    {
        this.myMachineId = myMachineId;
    }
    
    public void setLastCommittedTxId( long txId )
    {
        // Do nothing
    }
    
    public int getMyMachineId()
    {
        return this.myMachineId;
    }
    
    public void shutdown()
    {
        // Do nothing
    }

    public void invalidateMaster()
    {
    }
    
    public static BrokerFactory wrapSingleBroker( final Broker broker )
    {
        return new BrokerFactory()
        {
            public Broker create( String storeDir, Map<String, String> graphDbConfig )
            {
                return broker;
            }
        };
    }
}
