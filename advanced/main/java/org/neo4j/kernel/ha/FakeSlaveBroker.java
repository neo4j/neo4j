package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.ha.zookeeper.Machine;

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

    public Machine getMasterMachine()
    {
        return new Machine( masterMachineId, 0, 1, null );
    }

    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        throw new UnsupportedOperationException();
    }
}
