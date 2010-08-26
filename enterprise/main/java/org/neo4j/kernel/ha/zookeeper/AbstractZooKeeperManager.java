package org.neo4j.kernel.ha.zookeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.helpers.Pair;

/**
 * Contains basic functionality for a ZooKeeper manager, f.ex. how to get
 * the current master in the cluster.
 */
public abstract class AbstractZooKeeperManager implements Watcher
{
    protected static final String HA_SERVERS_CHILD = "ha-servers";
    protected static final int SESSION_TIME_OUT = 5000;
    
    private final String servers;
    private final Map<Integer, String> haServersCache = Collections.synchronizedMap(
            new HashMap<Integer, String>() );

    public AbstractZooKeeperManager( String servers )
    {
        this.servers = servers;
    }
    
    protected ZooKeeper instantiateZooKeeper()
    {
        try
        {
            return new ZooKeeper( servers, SESSION_TIME_OUT, this );
        }
        catch ( IOException e )
        {
            throw new ZooKeeperException( 
                "Unable to create zoo keeper client", e );
        }
    }
    
    protected abstract ZooKeeper getZooKeeper();
    
    public abstract String getRoot();
    
    protected Pair<Integer, Integer> parseChild( String child )
    {
        int index = child.indexOf( '_' );
        if ( index == -1 )
        {
            return null;
        }
        int id = Integer.parseInt( child.substring( 0, index ) );
        int seq = Integer.parseInt( child.substring( index + 1 ) );                
        return new Pair<Integer, Integer>( id, seq );
    }
    
    protected long readDataAsLong( String path ) throws InterruptedException, KeeperException
    {
        byte[] data = getZooKeeper().getData( path, false, null );
        ByteBuffer buf = ByteBuffer.wrap( data );
        return buf.getLong();
    }
    
    public Machine getMaster()
    {
        return getMasterBasedOn( getAllMachines().values() );
    }
    
    protected Machine getMasterBasedOn( Collection<Machine> machines )
    {
        Map<Integer, Pair<Long, Integer>> debugData = new TreeMap<Integer, Pair<Long, Integer>>();
        Machine master = null;
        int lowestSeq = Integer.MAX_VALUE;
        long highestTxId = -1;
        for ( Machine info : getAllMachines().values() )
        {
            debugData.put( info.getMachineId(),
                    new Pair<Long, Integer>( info.getLatestTxId(), info.getSequenceId() ) );
            if ( info.getLatestTxId() >= highestTxId )
            {
                highestTxId = info.getLatestTxId();
                if ( info.getSequenceId() < lowestSeq )
                {
                    master = info;
                    lowestSeq = info.getSequenceId();
                }
            }
        }
        System.out.println( "getMaster " + (master != null ? master.getMachineId() : "none") +
                " based on " + debugData );
        return master;
    }

    protected synchronized Map<Integer, Machine> getAllMachines()
    {
        waitForSyncConnected();
        try
        {
            Map<Integer, Machine> result = new HashMap<Integer, Machine>();
            String root = getRoot();
            List<String> children = getZooKeeper().getChildren( root, false );
            for ( String child : children )
            {
                Pair<Integer, Integer> parsedChild = parseChild( child );
                if ( parsedChild == null )
                {
                    continue;
                }
                
                try
                {
                    int id = parsedChild.first();
                    int seq = parsedChild.other();
                    long tx = readDataAsLong( root + "/" + child );
                    if ( !result.containsKey( id ) || seq > result.get( id ).getSequenceId() )
                    {
                        result.put( id, new Machine( id, seq, tx, getHaServer( id ) ) );
                    }
                }
                catch ( KeeperException inner )
                {
                    if ( inner.code() != KeeperException.Code.NONODE )
                    {
                        throw new ZooKeeperException( "Unabe to get master.", 
                            inner );
                    }
                }
            }
            return result;
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Unable to get master", e );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Interrupted.", e );
        }
    }
    
    protected String getHaServer( int machineId )
    {
        String result = haServersCache.get( machineId );
        if ( result == null )
        {
            result = readHaServer( machineId );
            haServersCache.put( machineId, result );
        }
        return result;
    }
    
    protected String readHaServer( int machineId )
    {
        waitForSyncConnected();
        String rootPath = getRoot();
        try
        {
            String haServerPath = rootPath + "/" + HA_SERVERS_CHILD + "/" + machineId;
            byte[] serverData = getZooKeeper().getData( haServerPath, false, null );
            ByteBuffer buffer = ByteBuffer.wrap( serverData );
            byte length = buffer.get();
            char[] chars = new char[length];
            buffer.asCharBuffer().get( chars );
            String result = String.valueOf( chars );
            System.out.println( "Read HA server:" + result + " (for machineID " + machineId +
                    ") from zoo keeper" );
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
    
    public void shutdown()
    {
//        new Exception( "shutdown zookeeper" ).printStackTrace();
        try
        {
            getZooKeeper().close();
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( 
                "Error closing zookeeper connection", e );
        }
    }
    
    protected abstract void waitForSyncConnected();
}
