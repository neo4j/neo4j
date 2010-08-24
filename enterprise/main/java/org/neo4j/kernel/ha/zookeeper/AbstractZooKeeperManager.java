package org.neo4j.kernel.ha.zookeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    protected static final int SESSION_TIME_OUT = 5000;
    
    private final String servers;
    private final String rootPath;

    public AbstractZooKeeperManager( String servers, long storeCreationTime, 
            long storeId )
    {
        this.servers = servers;
        this.rootPath = "/" + storeCreationTime + "_" + storeId;
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
    
    public String getRoot()
    {
        return this.rootPath;
    }
    
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
    
    public MachineInfo getMaster()
    {
        return getMasterBasedOn( getAllMachines().values() );
    }
    
    protected MachineInfo getMasterBasedOn( Collection<MachineInfo> machines )
    {
        MachineInfo master = null;
        int lowestSeq = Integer.MAX_VALUE;
        long highestTxId = -1;
        for ( MachineInfo info : getAllMachines().values() )
        {
            if ( info.latestTxId >= highestTxId )
            {
                highestTxId = info.latestTxId;
                if ( info.sequenceId < lowestSeq )
                {
                    master = info;
                    lowestSeq = info.sequenceId;
                }
            }
        }
        return master;
    }

    protected synchronized Map<Integer, MachineInfo> getAllMachines()
    {
        try
        {
            Map<Integer, MachineInfo> result = new HashMap<Integer, MachineInfo>();
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
                    if ( !result.containsKey( id ) || seq > result.get( id ).sequenceId )
                    {
                        result.put( id, new MachineInfo( id, seq, tx, getHaServer( id ) ) );
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
    
    protected abstract String getHaServer( int machineId );
    
    public void shutdown()
    {
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

    public static class MachineInfo
    {
        private final int machineId;
        private final int sequenceId;
        private final long latestTxId;
        private final String server;

        MachineInfo( int machineId, int sequenceId, long lastestTxId, String server )
        {
            this.machineId = machineId;
            this.sequenceId = sequenceId;
            this.latestTxId = lastestTxId;
            this.server = server;
        }

        public int getMachineId()
        {
            return machineId;
        }

        public long getLatestTxId()
        {
            return latestTxId;
        }

        public int getSequenceId()
        {
            return sequenceId;
        }

        public String getServer()
        {
            return server;
        }
        
        @Override
        public String toString()
        {
            return "MachineInfo[ID:" + machineId + ", sequence:" + sequenceId +
                    ", latest tx id:" + latestTxId + ", server:" + server + "]";
        }
        
        @Override
        public boolean equals( Object obj )
        {
            return (obj instanceof MachineInfo) && ((MachineInfo) obj).machineId == machineId;
        }
        
        @Override
        public int hashCode()
        {
            return machineId*19;
        }
    }
}
