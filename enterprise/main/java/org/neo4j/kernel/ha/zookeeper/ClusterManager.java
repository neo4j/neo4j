package org.neo4j.kernel.ha.zookeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ClusterManager implements Watcher
{
    private final ZooKeeper zooKeeper;
    private final long storeCreationTime;
    private final long storeId;

    public ClusterManager( String servers, long storeCreationTime, 
        long storeId )
    {
        try
        {
            this.zooKeeper = new ZooKeeper( servers, 5000, this );
        }
        catch ( IOException e )
        {
            throw new ZooKeeperException( 
                "Unable to create zoo keeper client", e );
        }
        this.storeCreationTime = storeCreationTime;
        this.storeId = storeId;
    }
    
    private String getRoot()
    {
        return "/" + storeCreationTime + "_" + storeId;
    }
    
    public void process( WatchedEvent event )
    {
        System.out.println( "Got event: " + event );
    }
    
    public synchronized int getMaster()
    {
        try
        {
            String root = getRoot();
            List<String> children = zooKeeper.getChildren( root, false );
            int currentMasterId = -1;
            int lowestSeq = Integer.MAX_VALUE;
            long highestTxId = -1;
            for ( String child : children )
            {
                int index = child.indexOf( '_' );
                int id = Integer.parseInt( child.substring( 0, index ) );
                int seq = Integer.parseInt( child.substring( index + 1 ) );                
                try
                {
                    byte[] data = zooKeeper.getData( root + "/" + child, false, 
                        null );
                    ByteBuffer buf = ByteBuffer.wrap( data );
                    long tx = buf.getLong();
                    if ( tx >= highestTxId )
                    {
                        highestTxId = tx;
                        if ( seq < lowestSeq )
                        {
                            currentMasterId = id;
                            lowestSeq = seq;
                        }
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
            return currentMasterId;
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
    
    public void dumpInfo()
    {
        try
        {
            String root = getRoot();
            List<String> children = zooKeeper.getChildren( root, false );
            for ( String child : children )
            {
                int index = child.indexOf( '_' );
                int id = Integer.parseInt( child.substring( 0, index ) );
                int seq = Integer.parseInt( child.substring( index + 1 ) );                
                try
                {
                    byte[] data = zooKeeper.getData( root + "/" + child, false, 
                        null );
                    ByteBuffer buf = ByteBuffer.wrap( data );
                    long tx = buf.getLong();
                    System.out.println( "machine=" + id + " (seq=" + seq + ") tx=" + tx );
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
    
    public void shutdown()
    {
        try
        {
            zooKeeper.close();
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( 
                "Error closing zookeeper connection", e );
        }
    }
    
    public void close()
    {
        try
        {
            zooKeeper.close();
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
    }
}
