package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;

public class FakeSlaveBroker extends AbstractBroker
{
    private final Master master;
    
    public FakeSlaveBroker()
    {
        this.master = new MasterClient( "localhost", CommunicationProtocol.PORT );
    }
    
    public Master getMaster()
    {
        return master;
    }
    
    @Override
    public boolean thisIsMaster()
    {
        return false;
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        throw new UnsupportedOperationException();
    }
}
