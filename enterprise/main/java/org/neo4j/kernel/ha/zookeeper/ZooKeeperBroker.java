package org.neo4j.kernel.ha.zookeeper;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.ResponseReceiver;

public class ZooKeeperBroker implements Broker
{
    private final ZooClient zooClient;
    private final int machineId;
    private final Map<Integer,String> haServers;
    
    private int machineIdForMasterClient;
    private MasterClient masterClient;
    
    public ZooKeeperBroker( String storeDir, int machineId, String zooKeeperServers, 
            Map<Integer,String> haServers, ResponseReceiver receiver )
    {
        this.machineId = machineId;
        this.haServers = haServers;
        NeoStoreUtil store = new NeoStoreUtil( storeDir ); 
        this.zooClient = new ZooClient( zooKeeperServers, machineId, 
                store.getCreationTime(), store.getStoreId(), store.getLastCommittedTx(),
                receiver );
    }
    
    public void invalidateMaster()
    {
        if ( masterClient != null )
        {
            masterClient.shutdown();
            masterClient = null;
        }
    }

    public Master getMaster()
    {
        if ( /*!zooClient.masterChanged() &&*/ masterClient != null )
        {
            return masterClient;
        }
        
        int masterId = zooClient.getMaster();
        if ( masterId == machineId )
        {
            throw new ZooKeeperException( "I am master, so can't call getMaster() here",
                    new Exception() );
        }
        invalidateMaster();
        createMaster( masterId );
        return masterClient;
    }

    private void createMaster( int masterId )
    {
        Pair<String, Integer> host = getHaServer( masterId );
        masterClient = new MasterClient( host.first(), host.other() );
    }

    private Pair<String, Integer> getHaServer( int machineId )
    {
        String host = haServers.get( machineId );
        if ( host == null )
        {
            throw new RuntimeException( "No HA server for machine ID " + machineId );
        }
        int pos = host.indexOf( ":" );
        return new Pair<String, Integer>( host.substring( 0, pos ),
                Integer.parseInt( host.substring( pos + 1 ) ) );
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb ),
                getHaServer( getMyMachineId() ).other() );
    }

    public void setLastCommittedTxId( long txId )
    {
        zooClient.setCommittedTx( txId );
    }
    
    public boolean thisIsMaster()
    {
        return zooClient.getMaster() == machineId;
    }
    
    public int getMyMachineId()
    {
        return machineId;
    }
    
    public void shutdown()
    {
        invalidateMaster();
        zooClient.shutdown();
    }
}
