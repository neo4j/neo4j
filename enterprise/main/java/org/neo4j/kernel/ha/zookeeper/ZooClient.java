/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.impl.util.StringLogger;

public class ZooClient extends AbstractZooKeeperManager
{
    static final String MASTER_NOTIFY_CHILD = "master-notify";
    static final String MASTER_REBOUND_CHILD = "master-rebound";
    
    private ZooKeeper zooKeeper;
    private final int machineId;
    private String sequenceNr;
    
    private long committedTx;
    
    private volatile KeeperState keeperState = KeeperState.Disconnected;
    private volatile boolean shutdown = false;
    private final ResponseReceiver receiver;
    private final String rootPath;
    private final String haServer;

    private final StringLogger msgLog;
    
    private long sessionId = -1;
    
    public ZooClient( String servers, int machineId, long storeCreationTime, 
        long storeId, long committedTx, ResponseReceiver receiver, String haServer, String storeDir )
    {
        super( servers, storeDir );
        this.rootPath = "/" + storeCreationTime + "_" + storeId;
        this.haServer = haServer;
        this.receiver = receiver;
        this.machineId = machineId;
        this.committedTx = committedTx;
        this.sequenceNr = "not initialized yet";
        this.msgLog = StringLogger.getLogger( storeDir + "/messages.log" );
        this.zooKeeper = instantiateZooKeeper();
    }
    
    @Override
    protected int getMyMachineId()
    {
        return this.machineId;
    }
    
    public void process( WatchedEvent event )
    {
        try
        {
            String path = event.getPath();
            msgLog.logMessage( this + ", " + new Date() + " Got event: " + event + "(path=" + path + ")", true );
            if ( path == null && event.getState() == Watcher.Event.KeeperState.Expired )
            {
                keeperState = KeeperState.Expired;
                if ( zooKeeper != null )
                {
                    try
                    {
                        zooKeeper.close();
                    }
                    catch ( InterruptedException e )
                    {
                        e.printStackTrace();
                        Thread.interrupted();
                    }
                }
                zooKeeper = instantiateZooKeeper();
            }
            else if ( path == null && event.getState() == Watcher.Event.KeeperState.SyncConnected )
            {
                long newSessionId = zooKeeper.getSessionId();
                Pair<Master, Machine> masterBeforeIWrite = getMasterFromZooKeeper( false );
                msgLog.logMessage( "Get master before write:" + masterBeforeIWrite );
                if ( newSessionId != sessionId || masterBeforeIWrite.other().getMachineId() != getCachedMaster().other().getMachineId() )
                {
                    sequenceNr = setup();
                    msgLog.logMessage( "Did setup, seq=" + sequenceNr + " new sessionId=" + newSessionId );
                    keeperState = KeeperState.SyncConnected;
                    Pair<Master, Machine> masterAfterIWrote = getMasterFromZooKeeper( false );
                    msgLog.logMessage( "Get master after write:" + masterAfterIWrote );
                    int masterId = masterAfterIWrote.other().getMachineId();
    //                if ( masterBeforeIWrite.other().getMachineId() != masterId && masterId != machineId )
    //                {
                        setDataChangeWatcher( MASTER_NOTIFY_CHILD, masterId );
    //                }
                    receiver.newMaster( masterAfterIWrote, new Exception() );
                    sessionId = newSessionId;
                }
                else
                {
                    msgLog.logMessage( "SyncConnected with same session id: " + sessionId );
                    keeperState = KeeperState.SyncConnected;
                }
            }
            else if ( path == null && event.getState() == Watcher.Event.KeeperState.Disconnected )
            {
                keeperState = KeeperState.Disconnected;
            }
            else if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
            {
                Pair<Master, Machine> currentMaster = getMasterFromZooKeeper( true );
                if ( path.contains( MASTER_NOTIFY_CHILD ) )
                {
                    setDataChangeWatcher( MASTER_NOTIFY_CHILD, -1 );
                    if ( currentMaster.other().getMachineId() == machineId )
                    {
                        receiver.newMaster( currentMaster, new Exception() );
                    }
                }
                else if ( path.contains( MASTER_REBOUND_CHILD ) )
                {
                    setDataChangeWatcher( MASTER_REBOUND_CHILD, -1 );
                    if ( currentMaster.other().getMachineId() != machineId )
                    {
                        receiver.newMaster( currentMaster, new Exception() );
                    }
                }
                else
                {
                    msgLog.logMessage( "Unrecognized data change " + path );
                }
            }
        }
        catch ( RuntimeException e )
        {
            msgLog.logMessage( "Error in ZooClient.process", e, true );
            e.printStackTrace();
            throw e;
        }
        finally
        {
            msgLog.flush();
        }
    }
    
    public void waitForSyncConnected()
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
    
    protected void setDataChangeWatcher( String child, int currentMasterId )
    {
        try
        {
            String root = getRoot();
            String path = root + "/" + child;
            byte[] data = null;
            boolean exists = false;
            try
            { 
                data = zooKeeper.getData( path, true, null );
                exists = true;
                int id = ByteBuffer.wrap( data ).getInt();
                if ( currentMasterId == -1 || id == currentMasterId )
                {
                    //System.out.println( child + " not set, is already " + currentMasterId );
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
                data = new byte[4];
                ByteBuffer.wrap( data ).putInt( currentMasterId );
                if ( !exists )
                {
                    zooKeeper.create( path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                            CreateMode.PERSISTENT );
                    msgLog.logMessage( child + " created with " + currentMasterId );
                }
                else if ( currentMasterId != -1 )
                {
                    zooKeeper.setData( path, data, -1 );
                    msgLog.logMessage( child + " set to " + currentMasterId );
                }
                
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
                Pair<Integer, Integer> parsedChild = parseChild( child );
                if ( parsedChild == null )
                {
                    continue;
                }
                if ( parsedChild.first() == machineId )
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
    
    private byte[] dataRepresentingMe( long txId )
    {
        byte[] array = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap( array );
        buffer.putLong( txId );
        return array;
    }
    
    private String setup()
    {
        try
        {
            cleanupChildren();
            writeHaServerConfig();
            String root = getRoot();
            String path = root + "/" + machineId + "_";
            String created = zooKeeper.create( path, dataRepresentingMe( committedTx ), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );
            
            // Add watches to our master notification nodes
            setDataChangeWatcher( MASTER_NOTIFY_CHILD, -1 );
            setDataChangeWatcher( MASTER_REBOUND_CHILD, -1 );
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

    private void writeHaServerConfig() throws InterruptedException, KeeperException
    {
        // Make sure the HA server root is created
        String path = rootPath + "/" + HA_SERVERS_CHILD;
        try
        {
            zooKeeper.create( path, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
        }
        catch ( KeeperException e )
        {
            if ( e.code() != KeeperException.Code.NODEEXISTS )
            {
                throw e;
            }
        }
        
        // Write the HA server config.
        String machinePath = path + "/" + machineId;
        byte[] data = haServerAsData();
        try
        {
            zooKeeper.create( machinePath, data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL );
        }
        catch ( KeeperException e )
        {
            if ( e.code() != KeeperException.Code.NODEEXISTS )
            {
                throw e;
            }
            msgLog.logMessage( "HA server info already present, trying again" );
            try
            {
                zooKeeper.delete( machinePath, -1 );
            }
            catch ( KeeperException ee )
            {
                ee.printStackTrace();
                // ok
            }
            finally
            {
                writeHaServerConfig();
            }
        }
        zooKeeper.setData( machinePath, data, -1 );
        msgLog.logMessage( "Wrote HA server " + haServer + " to zoo keeper" );
    }

    private byte[] haServerAsData()
    {
        byte[] array = new byte[haServer.length()*2 + 20];
        ByteBuffer buffer = ByteBuffer.wrap( array );
        buffer.put( (byte) haServer.length() );
        buffer.asCharBuffer().put( haServer.toCharArray() ).flip();
        byte[] actualArray = new byte[buffer.limit()];
        System.arraycopy( array, 0, actualArray, 0, actualArray.length );
        return actualArray;
    }
    
    public synchronized void setCommittedTx( long tx )
    {
        msgLog.logMessage( "ZooClient setting txId=" + tx + " for machine=" + machineId, true );
        waitForSyncConnected();
        this.committedTx = tx;
        String root = getRoot();
        String path = root + "/" + machineId + "_" + sequenceNr;
        byte[] data = dataRepresentingMe( tx );
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
    protected String getHaServer( int machineId, boolean wait )
    {
        return machineId == this.machineId ? haServer : super.getHaServer( machineId, wait );
    }
}
