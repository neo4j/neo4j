package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;

public class FakeMasterBroker extends AbstractBroker
{
    public FakeMasterBroker( int myMachineId )
    {
        super( myMachineId );
    }
    
    public int getMasterMachineId()
    {
        return getMyMachineId();
    }
    
    public Master getMaster()
    {
        throw new UnsupportedOperationException( "I am master" );
    }

    public boolean thisIsMaster()
    {
        return true;
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb ), CommunicationProtocol.PORT );
    }
}
