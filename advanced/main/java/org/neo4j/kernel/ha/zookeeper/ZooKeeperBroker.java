package org.neo4j.kernel.ha.zookeeper;

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
    private final String haServer;
    private MasterClient masterClient;
    
    public ZooKeeperBroker( String storeDir, int machineId, String zooKeeperServers, 
            String haServer, ResponseReceiver receiver )
    {
        this.machineId = machineId;
        this.haServer = haServer;
        NeoStoreUtil store = new NeoStoreUtil( storeDir ); 
        this.zooClient = new ZooClient( zooKeeperServers, machineId, store.getCreationTime(),
                store.getStoreId(), store.getLastCommittedTx(), receiver, haServer );
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
        if ( masterClient != null )
        {
            return masterClient;
        }
        
        Machine master = zooClient.getMaster();
        if ( master != null && master.getMachineId() == machineId )
        {
            throw new ZooKeeperException( "I am master, so can't call getMaster() here",
                    new Exception() );
        }
        invalidateMaster();
        createMaster( master );
        return masterClient;
    }

    private void createMaster( Machine machine )
    {
        Pair<String, Integer> host = machine != null ? machine.getServer() :
                new Pair<String, Integer>( null, -1 );
        masterClient = new MasterClient( host.first(), host.other() );
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb ),
                Machine.splitIpAndPort( haServer ).other() );
    }

    public void setLastCommittedTxId( long txId )
    {
        zooClient.setCommittedTx( txId );
    }
    
    public boolean thisIsMaster()
    {
        Machine master = zooClient.getMaster();
        return master != null && master.getMachineId() == machineId;
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
