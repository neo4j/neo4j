package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;

public class FakeSlaveBroker extends AbstractBroker
{
    private final Master master;
    private final int masterMachineId;

    public FakeSlaveBroker( Master master, int masterMachineId, int myMachineId )
    {
        super( myMachineId );
        this.masterMachineId = masterMachineId;
        this.master = master;
    }

    public Master getMaster()
    {
        return master;
    }

    public boolean thisIsMaster()
    {
        return false;
    }

    public int getMasterMachineId()
    {
        return this.masterMachineId;
    }

    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        throw new UnsupportedOperationException();
    }
}
