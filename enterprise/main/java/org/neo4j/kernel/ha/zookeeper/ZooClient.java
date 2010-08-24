package org.neo4j.kernel.ha.zookeeper;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.kernel.impl.ha.ResponseReceiver;

public class ZooClient extends AbstractZooKeeperManager
{
    private static final String MASTER_NOTIFY_CHILD = "master-notify";
    
    private ZooKeeper zooKeeper;
    private final int machineId;
    private String sequenceNr;
    
    private long committedTx;
    
    private volatile KeeperState keeperState = KeeperState.Disconnected;
    private volatile boolean shutdown = false;
    private final ResponseReceiver receiver;
    
    public ZooClient( String servers, int machineId, long storeCreationTime, 
        long storeId, long committedTx, ResponseReceiver receiver )
    {
        super( servers, storeCreationTime, storeId );
        this.zooKeeper = instantiateZooKeeper();
        this.receiver = receiver;
        this.machineId = machineId;
        this.committedTx = committedTx;
        this.sequenceNr = "not initialized yet";
    }
    
    public void process( WatchedEvent event )
    {
        String path = event.getPath();
        System.out.println( this + ", " + new Date() + " Got event: " + event + "(path=" + path + ")" );
        if ( path == null && event.getState() == Watcher.Event.KeeperState.Expired )
        {
            keeperState = KeeperState.Expired;
            instantiateZooKeeper();
        }
        else if ( path == null && event.getState() == Watcher.Event.KeeperState.SyncConnected )
        {
            sequenceNr = setup();
            keeperState = KeeperState.SyncConnected;
            receiver.somethingIsWrong( new Exception() );
        }
        else if ( path == null && event.getState() == Watcher.Event.KeeperState.Disconnected )
        {
            keeperState = KeeperState.Disconnected;
        }
        else if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
        {
            System.out.println( "NodeDataChanged (most likely master-notify)" );
            receiver.somethingIsWrong( new Exception() );
        }
    }
    
    private void waitForSyncConnected()
    {
        if ( keeperState == KeeperState.SyncConnected )
        {
            return;
        }
        if ( shutdown == true )
        {
            throw new ZooKeeperException( "ZooKeeper client has been shutdwon" );
        }
        long startTime = System.currentTimeMillis();
        long currentTime = startTime;
        synchronized ( keeperState )
        {
            do
            {
                try
                {
                    keeperState.wait( 250 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                if ( keeperState == KeeperState.SyncConnected )
                {
                    return;
                }
                if ( shutdown == true )
                {
                    throw new ZooKeeperException( "ZooKeeper client has been shutdwon" );
                }
                currentTime = System.currentTimeMillis();
            }
            while ( (currentTime - startTime) < SESSION_TIME_OUT );
            if ( keeperState != KeeperState.SyncConnected )
            {
                throw new ZooKeeperTimedOutException( 
                        "Connection to ZooKeeper server timed out, keeper state=" + keeperState );
            }
        }
    }
    
    private void setMasterChangeWatcher( int currentMasterId )
    {
        try
        {
            String root = getRoot();
            String path = root + "/" + MASTER_NOTIFY_CHILD;
            byte[] data = null;
            boolean exists = false;
            try
            { 
                data = zooKeeper.getData( path, true, null );
                exists = true;
                if ( data[0] == currentMasterId )
                {
                    return;
                }
            }
            catch ( KeeperException e )
            {
                if ( e.code() != KeeperException.Code.NONODE )
                {
                    throw new ZooKeeperException( "Couldn't get master notify node", e );
                }
            }
            
            // Didn't exist or has changed
            try
            {
                data = new byte[] { (byte) currentMasterId };
                if ( !exists )
                {
                    zooKeeper.create( path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                            CreateMode.PERSISTENT );
                }
                zooKeeper.setData( path, data, -1 );
                System.out.println( "master-notify set to " + currentMasterId );
                
                // Add a watch for it
                zooKeeper.getData( path, true, null );
            }
            catch ( KeeperException e )
            {
                if ( e.code() != KeeperException.Code.NODEEXISTS )
                {
                    throw new ZooKeeperException( "Couldn't set master notify node", e );
                }
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Interrupted", e );
        }
    }
    
    public String getRoot()
    {
        String rootPath = super.getRoot();
        
        // Make sure it exists
        byte[] rootData = null;
        do
        {
            try
            {
                rootData = zooKeeper.getData( rootPath, false, null );
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
                byte data[] = new byte[0];
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
    
    private void cleanupChildren()
    {
        try
        {
            String root = getRoot();
            List<String> children = zooKeeper.getChildren( root, false );
            for ( String child : children )
            {
                if ( child.equals( MASTER_NOTIFY_CHILD ) )
                {
                    continue;
                }
                int index = child.indexOf( '_' );
                int id = Integer.parseInt( child.substring( 0, index ) );
                if ( id == machineId )
                {
                    zooKeeper.delete( root + "/" + child, -1 );
                }
            }
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Unable to clean up old child", e );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Interrupted.", e );
        }
    }
    
    private String setup()
    {
        try
        {
            cleanupChildren();
            String root = getRoot();
            String path = root + "/" + machineId + "_";
            byte[] data = new byte[8];
            ByteBuffer buf = ByteBuffer.wrap( data );
            buf.putLong( committedTx );
            String created = zooKeeper.create( path, data, 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );
            System.out.println( this + " wrote " + committedTx + " to zookeeper" );
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
        catch ( Throwable t )
        {
            t.printStackTrace();
            throw new ZooKeeperException( "Unknown setup error", t );
        }
    }

    public synchronized MachineInfo getMaster()
    {
        waitForSyncConnected();
        MachineInfo result = super.getMaster();
        if ( result != null )
        {
            setMasterChangeWatcher( result.getMachineId() );
        }
        return result;
    }
    
    public synchronized void setCommittedTx( long tx )
    {
        System.out.println( "Setting txId=" + tx + " for machine=" + machineId );
        waitForSyncConnected();
        if ( tx <= committedTx )
        {
            throw new IllegalArgumentException( "tx=" + tx + 
                " but committedTx is " + committedTx );
        }
        this.committedTx = tx;
        String root = getRoot();
        String path = root + "/" + machineId + "_" + sequenceNr;
        byte[] data = new byte[8];
        ByteBuffer buf = ByteBuffer.wrap( data );
        buf.putLong( tx );
        try
        {
            zooKeeper.setData( path, data, -1 );
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
        this.shutdown = true;
        super.shutdown();
    }

    @Override
    protected ZooKeeper getZooKeeper()
    {
        return zooKeeper;
    }

    @Override
    protected String getHaServer( int machineId )
    {
        return "";
    }
}
