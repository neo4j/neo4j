package org.neo4j.kernel.ha.zookeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZooClient implements Watcher
{
    private final ZooKeeper zooKeeper;
    private final long storeCreationTime;
    private final long storeId;
    private final int machineId;
    private final String sequenceNr;
    
    private int committedTx;
    private int globalCommittedTx;
    
    public ZooClient( String servers, int machineId, long storeCreationTime, 
        long storeId, int committedTx )
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
        this.machineId = machineId;
        this.storeCreationTime = storeCreationTime;
        this.storeId = storeId;
        this.committedTx = committedTx;
        sequenceNr = setup();
    }
    
    private String getRoot()
    {
        String rootPath = "/" + storeCreationTime + "_" + storeId;
        byte[] rootData = null;
        do
        {
            try
            {
                rootData = zooKeeper.getData( rootPath, false, null );
                ByteBuffer buf = ByteBuffer.wrap( rootData );
                globalCommittedTx = buf.getInt();
                if ( globalCommittedTx < committedTx )
                {
                    throw new IllegalStateException( "Global committed tx " + 
                        globalCommittedTx + " while machine[" + machineId + 
                        "] @" + committedTx );
                }
                // ok we got the root
                return rootPath;
            }
            catch ( KeeperException e )
            {
                if ( e.code() != KeeperException.Code.NONODE )
                {
                    throw new ZooKeeperException( "Unable to get root node", 
                        e ); 
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new ZooKeeperException( "Got interrupted", e );
            }
            // try create root
            try
            {
                byte data[] = new byte[4];
                ByteBuffer buf = ByteBuffer.wrap( data );
                buf.putInt( committedTx );
                zooKeeper.create( rootPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                    CreateMode.PERSISTENT );
            }
            catch ( KeeperException e )
            {
                if ( e.code() != KeeperException.Code.NODEEXISTS )
                {
                    throw new ZooKeeperException( "Unable to create root", e );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new ZooKeeperException( "Got interrupted", e );
            }
        } while ( rootData == null );
        throw new IllegalStateException();
    }
    
    private String setup()
    {
        try
        {
            String path = getRoot() + "/" + machineId + "_";
            byte[] data = new byte[4];
            ByteBuffer buf = ByteBuffer.wrap( data );
            buf.putInt( committedTx );
            String created = zooKeeper.create( path, data, 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );
            return created.substring( created.lastIndexOf( "_" ) + 1 );
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Unable to setup", e );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Setup got interrupted", e );
        }
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
                    int tx = buf.getInt();
                    if ( tx == globalCommittedTx )
                    {
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
    
    public synchronized void setCommittedTx( int tx )
    {
        if ( tx <= committedTx )
        {
            throw new IllegalArgumentException( "tx=" + tx + 
                " but committedTx is " + committedTx );
        }
        int masterId = getMaster();
        String root = getRoot();
        String path = root + "/" + machineId + "_" + sequenceNr;
        byte[] data = new byte[4];
        ByteBuffer buf = ByteBuffer.wrap( data );
        buf.putInt( tx );
        try
        {
            zooKeeper.setData( path, data, -1 );
            if ( masterId == machineId )
            {
                zooKeeper.setData( root, data, -1 );
            }
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Unable to set current tx", e );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Interrupted...", e );
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
    
    public static void main( String args[] )
    {
        String servers = "localhost:2181,localhost:2182,localhost:2183";
        ZooClient client = new ZooClient( servers, 1, 2, 1, 0 );
        System.out.println( client.getMaster() );
        client.setCommittedTx( 1 );
        System.out.println( client.getMaster() );
        client.shutdown();
    }
}
