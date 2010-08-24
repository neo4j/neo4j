package org.neo4j.kernel.ha.zookeeper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public class ClusterManager extends AbstractZooKeeperManager
{
    private final ZooKeeper zooKeeper;
    private final Map<Integer, String> haServers;
    
    public ClusterManager( String servers, long storeCreationTime, 
        long storeId, String haServers )
    {
        super( servers, storeCreationTime, storeId );
        this.zooKeeper = instantiateZooKeeper();
        this.haServers = HighlyAvailableGraphDatabase.parseHaServersConfig( haServers );
    }
    
    public void process( WatchedEvent event )
    {
        System.out.println( "Got event: " + event );
    }
    
    /**
     * Returns the disconnected slaves in this cluster so that all slaves
     * which are specified in the HA servers configuration, but not in the
     * zoo keeper cluster will be returned.
     * @return the disconnected slaves in this cluster.
     */
    public MachineInfo[] getDisconnectedSlaves()
    {
        Collection<MachineInfo> infos = new ArrayList<MachineInfo>();
        for ( Map.Entry<Integer, String> entry : haServers.entrySet() )
        {
            infos.add( new MachineInfo( entry.getKey(), -1, -1, entry.getValue() ) );
        }
        infos.removeAll( Arrays.asList( getAllMachines() ) );
        return infos.toArray( new MachineInfo[infos.size()] );
    }
    
    /**
     * Returns the connected slaves in this cluster.
     * @return the connected slaves in this cluster.
     */
    public MachineInfo[] getConnectedSlaves()
    {
        Map<Integer, MachineInfo> machines = getAllMachines();
        MachineInfo master = getMasterBasedOn( machines.values() );
        Collection<MachineInfo> result = new ArrayList<MachineInfo>( machines.values() );
        result.remove( master );
        return result.toArray( new MachineInfo[result.size()] );
    }
    
    @Override
    protected ZooKeeper getZooKeeper()
    {
        return this.zooKeeper;
    }
    
    @Override
    protected String getHaServer( int machineId )
    {
        String server = haServers.get( machineId );
        return server != null ? server :
                "No HA server config specified for machine ID " + machineId;
    }
}
