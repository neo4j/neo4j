package org.neo4j.kernel.ha.zookeeper;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Master;

public class ZooKeeperBroker implements Broker
{
    private final ZooClient zooClient;
    private final String storeDir;
    private final int machineId;
    private final String zooKeeperServers;
    private final Map<Integer,String> haServers;
    
    private int machineIdForMasterClient;
    private MasterClient masterClient;
    
    public ZooKeeperBroker( String storeDir, int machineId, String zooKeeperServers, 
            Map<Integer,String> haServers )
    {
        this.storeDir = storeDir;
        this.machineId = machineId;
        this.zooKeeperServers = zooKeeperServers;
        this.haServers = haServers;
        NeoStoreUtil store = new NeoStoreUtil( storeDir ); 
        this.zooClient = new ZooClient( zooKeeperServers, machineId, 
                store.getCreationTime(), store.getStoreId(), store.getLastCommittedTx() );
        
        int masterId = zooClient.getMaster();
        if ( masterId != this.machineId )
        {
            getAndCacheMaster( masterId );
        }
    }

    public Master getMaster()
    {
        int masterId = zooClient.getMaster();
        if ( masterId == machineId )
        {
            throw new RuntimeException( "I am master" );
        }
        return getAndCacheMaster( masterId );
    }

    private Master getAndCacheMaster( int masterId )
    {
        if ( masterClient == null || masterId != machineIdForMasterClient )
        {
            String host = haServers.get( masterId );
            machineIdForMasterClient = masterId;
            // TODO synchronization
            masterClient = new MasterClient( host, getHaServerPort( masterId ) );
        }
        return masterClient;
    }
    
    private int getHaServerPort( int machineId )
    {
        String host = haServers.get( getMyMachineId() );
        return Integer.parseInt( host.substring( host.indexOf( ":" ) + 1 ) );
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb ), getHaServerPort( getMyMachineId() ) );
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
        return zooClient.getMaster();
    }
}
