package org.neo4j.kernel.ha;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class FakeMasterBroker extends AbstractBroker
{
    public FakeMasterBroker( int myMachineId, String storeDir )
    {
        super( myMachineId, storeDir );
    }
    
    public Machine getMasterMachine()
    {
        return new Machine( getMyMachineId(), 0, 1, null );
    }
    
    public Pair<Master, Machine> getMaster()
    {
        return new Pair<Master,Machine>( null, new Machine( getMyMachineId(), 0, 1, null ) );
        // throw new UnsupportedOperationException( "I am master" );
    }

    public Pair<Master, Machine> getMasterReally()
    {
        return new Pair<Master,Machine>( null, new Machine( getMyMachineId(), 0, 1, null ) );
    }
    
    public boolean iAmMaster()
    {
        return getMyMachineId() == 0;
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb ), CommunicationProtocol.PORT, getStoreDir() );
    }
}
