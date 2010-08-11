package org.neo4j.kernel.ha.zookeeper;

import java.util.Map;

import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.SlaveContext;

public class ZooKeeperBroker implements Broker
{
    private final ZooClient zooClient;
    private final String storeDir;
    private final int machineId;
    private final String zooKeeperServers;
    private final Map<Integer,String> haServers;
    
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
    }

    
    public Master getMaster()
    {
        int masterId = zooClient.getMaster();
        if ( masterId == machineId )
        {
            throw new RuntimeException( "I am master" );
        }
        String host = haServers.get( masterId );
        // TODO
        // return new MasterClient( host );
        return null;
    }

    public SlaveContext getSlaveContext()
    {
        throw new RuntimeException( "Move this method somewhere else" );
    }
    
    public void setLastCommittedTxId( long txId )
    {
        zooClient.setCommittedTx( txId );
    }
    
    public boolean thisIsMaster()
    {
        return zooClient.getMaster() == machineId;
    }
}
