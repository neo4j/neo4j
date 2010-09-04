package org.neo4j.kernel.ha.zookeeper;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.ResponseReceiver;

public class ZooKeeperBroker extends AbstractBroker
{
    private final ZooClient zooClient;
    private final String haServer;
    private final int machineId;
    
    public ZooKeeperBroker( String storeDir, int machineId, String zooKeeperServers, 
            String haServer, ResponseReceiver receiver )
    {
        super( machineId );
        this.machineId = machineId;
        this.haServer = haServer;
        NeoStoreUtil store = new NeoStoreUtil( storeDir ); 
        this.zooClient = new ZooClient( zooKeeperServers, machineId, store.getCreationTime(),
                store.getStoreId(), store.getLastCommittedTx(), receiver, haServer );
    }
    
    public Pair<Master, Machine> getMaster()
    {
        return zooClient.getCachedMaster();
    }
    
    public Pair<Master, Machine> getMasterReally()
    {
        return zooClient.getMasterFromZooKeeper( true );
    }
    
    public Machine getMasterExceptMyself()
    {
        Map<Integer, Machine> machines = zooClient.getAllMachines( true );
        machines.remove( this.machineId );
        return zooClient.getMasterBasedOn( machines.values() );
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        MasterServer server = new MasterServer( new MasterImpl( graphDb ),
                Machine.splitIpAndPort( haServer ).other() );
        return server;
    }

    public void setLastCommittedTxId( long txId )
    {
        zooClient.setCommittedTx( txId );
    }
    
    public boolean iAmMaster()
    {
        return zooClient.getCachedMaster().other().getMachineId() == getMyMachineId();
    }
    
    public void shutdown()
    {
        zooClient.shutdown();
    }
    
    public void rebindMaster()
    {
        zooClient.setDataChangeWatcher( ZooClient.MASTER_REBOUND_CHILD, machineId );
    }
}
