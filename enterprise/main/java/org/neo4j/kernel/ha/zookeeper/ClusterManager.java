package org.neo4j.kernel.ha.zookeeper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ClusterManager extends AbstractZooKeeperManager
{
    private final ZooKeeper zooKeeper;
    private String rootPath;
    private KeeperState state = KeeperState.Disconnected;
    private Map<Integer, String> haServers;
    
    public ClusterManager( String servers )
    {
        super( servers );
        this.zooKeeper = instantiateZooKeeper();
    }
    
    private void waitForSyncConnected()
    {
        long startTime = System.currentTimeMillis();
        while ( System.currentTimeMillis()-startTime < SESSION_TIME_OUT )
        {
            if ( state == KeeperState.SyncConnected )
            {
                // We are connected
                break;
            }
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }

    public void process( WatchedEvent event )
    {
        System.out.println( "Got event: " + event );
        String path = event.getPath();
        if ( path == null )
        {
            state = event.getState();
        }
    }
    
    @Override
    public String getRoot()
    {
        if ( rootPath == null )
        {
            rootPath = readRootPath();
        }
        return rootPath;
    }
    
    private String readRootPath()
    {
        waitForSyncConnected();
        try
        {
            List<String> children = getZooKeeper().getChildren( "/", false );
            String foundChild = null;
            for ( String child : children )
            {
                if ( child.contains( "_" ) )
                {
                    if ( foundChild != null )
                    {
                        throw new RuntimeException( "Multiple roots found, " +
                                foundChild + " and " + child );
                    }
                    foundChild = child;
                }
            }
            
            if ( foundChild != null )
            {
                System.out.println( "Read root path " + foundChild + " from zoo keeper" );
                return foundChild;
            }
            throw new RuntimeException( "No root child found in zoo keeper" );
        }
        catch ( KeeperException e )
        {
            throw new RuntimeException( e );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Returns the disconnected slaves in this cluster so that all slaves
     * which are specified in the HA servers configuration, but not in the
     * zoo keeper cluster will be returned.
     * @return the disconnected slaves in this cluster.
     */
    public Machine[] getDisconnectedSlaves()
    {
        Collection<Machine> infos = new ArrayList<Machine>();
        for ( Map.Entry<Integer, String> entry : haServers.entrySet() )
        {
            infos.add( new Machine( entry.getKey(), -1, -1, entry.getValue() ) );
        }
        infos.removeAll( getAllMachines().values() );
        return infos.toArray( new Machine[infos.size()] );
    }
    
    /**
     * Returns the connected slaves in this cluster.
     * @return the connected slaves in this cluster.
     */
    public Machine[] getConnectedSlaves()
    {
        Map<Integer, Machine> machines = getAllMachines();
        Machine master = getMasterBasedOn( machines.values() );
        Collection<Machine> result = new ArrayList<Machine>( machines.values() );
        result.remove( master );
        return result.toArray( new Machine[result.size()] );
    }
    
    @Override
    protected ZooKeeper getZooKeeper()
    {
        return this.zooKeeper;
    }
    
    @Override
    protected String getHaServer( int machineId )
    {
        if ( haServers == null )
        {
            haServers = readHaServers();
        }
        
        String server = haServers.get( machineId );
        return server != null ? server :
                "No HA server config specified for machine ID " + machineId;
    }

    private Map<Integer, String> readHaServers()
    {
        waitForSyncConnected();
        Map<Integer, String> result = new HashMap<Integer, String>();
        String rootPath = getRoot();
        try
        {
            String haRootPath = rootPath + "/" + HA_SERVERS_CHILD;
            for ( String child : zooKeeper.getChildren( rootPath + "/" + HA_SERVERS_CHILD, false ) )
            {
                byte[] serverData = zooKeeper.getData( child, false, null );
                ByteBuffer buffer = ByteBuffer.wrap( serverData );
                byte length = buffer.get();
                char[] chars = new char[length];
                buffer.asCharBuffer().get( chars );
                result.put( Integer.parseInt( child ), String.valueOf( chars ) );
            }
            System.out.println( "Read HA servers:" + result + " from zoo keeper" );
            return result;
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Couldn't find the HA servers root node", e );
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( "Interrupted", e );
        }
    }
}
