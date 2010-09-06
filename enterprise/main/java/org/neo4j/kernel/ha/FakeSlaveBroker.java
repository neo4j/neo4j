package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class FakeSlaveBroker extends AbstractBroker
{
    private final Master master;
    private final Machine masterMachine;

    public FakeSlaveBroker( Master master, int masterMachineId, int myMachineId, String storeDir )
    {
        super( myMachineId, storeDir );
        this.master = master;
        this.masterMachine = new Machine( masterMachineId, 0, 1, null );
    }

    public Pair<Master, Machine> getMaster()
    {
        return new Pair<Master, Machine>( master, Machine.NO_MACHINE );
    }
    
    public Pair<Master, Machine> getMasterReally()
    {
        return new Pair<Master, Machine>( master, Machine.NO_MACHINE );
    }
    
    public boolean iAmMaster()
    {
        return false;
    }

    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        throw new UnsupportedOperationException();
    }
}
