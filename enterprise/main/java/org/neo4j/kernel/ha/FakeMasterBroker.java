package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;

public class FakeMasterBroker extends AbstractBroker
{
    private final int machineId;

    public FakeMasterBroker( int machineId )
    {
        this.machineId = machineId;
    }
    
    @Override
    public void setDb( GraphDatabaseService db )
    {
        super.setDb( db );
    }
    
    @Override
    public int getMasterMachineId()
    {
        return this.machineId;
    }
    
    @Override
    public int getMyMachineId()
    {
        return this.machineId;
    }
    
    public Master getMaster()
    {
        throw new UnsupportedOperationException( "I am master" );
    }

    @Override
    public boolean thisIsMaster()
    {
        return true;
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb ), CommunicationProtocol.PORT );
    }
}
