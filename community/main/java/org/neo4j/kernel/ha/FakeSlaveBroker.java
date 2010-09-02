package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;

public class FakeSlaveBroker extends AbstractBroker
{
    private final Master master;
    private final int masterMachineId;
    private final int machineId;
    
    public FakeSlaveBroker( int masterMachineId, int machineId )
    {
        this.masterMachineId = masterMachineId;
        this.machineId = machineId;
        this.master = new MasterClient( "localhost", CommunicationProtocol.PORT );
    }
    
    public Master getMaster()
    {
        return master;
    }
    
    @Override
    public int getMasterMachineId()
    {
        return this.masterMachineId;
    }
    
    @Override
    public int getMyMachineId()
    {
        return this.machineId;
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
