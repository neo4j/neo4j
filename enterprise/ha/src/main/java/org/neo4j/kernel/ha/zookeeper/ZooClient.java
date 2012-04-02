/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.kernel.ha.HaSettings.allow_init_cluster;
import static org.neo4j.kernel.ha.HaSettings.cluster_name;
import static org.neo4j.kernel.ha.HaSettings.lock_read_timeout;
import static org.neo4j.kernel.ha.HaSettings.max_concurrent_channels_per_slave;
import static org.neo4j.kernel.ha.HaSettings.read_timeout;
import static org.neo4j.kernel.ha.HaSettings.server;
import static org.neo4j.kernel.ha.HaSettings.server_id;
import static org.neo4j.kernel.ha.HaSettings.slave_coordinator_update_mode;
import static org.neo4j.kernel.ha.HaSettings.zk_session_timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import javax.management.remote.JMXServiceURL;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.com.StoreIdGetter;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.SlaveUpdateMode;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.ClusterEventReceiver;
import org.neo4j.kernel.ha.ConnectionInformation;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.SlaveDatabaseOperations;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.NullLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.StringLogger;

public class ZooClient extends AbstractZooKeeperManager
{
    static final String MASTER_NOTIFY_CHILD = "master-notify";
    static final String MASTER_REBOUND_CHILD = "master-rebound";

    private ZooKeeper zooKeeper;
    private final int machineId;
    private String sequenceNr;

    private long committedTx;
    private int masterForCommittedTx;

    private final Object keeperStateMonitor = new Object();
    private volatile KeeperState keeperState = KeeperState.Disconnected;
    private volatile boolean shutdown = false;
    private String rootPath;
    private volatile StoreId storeId;

    // Has the format <host-name>:<port>
    private final String haServer;

    private final String storeDir;
    private long sessionId = -1;
    private Config conf;
    private final SlaveDatabaseOperations localDatabase;
    private final ClusterEventReceiver clusterReceiver;
    private final int backupPort;
    private final boolean writeLastCommittedTx;
    private final String clusterName;
    private final boolean allowCreateCluster;

    public ZooClient( String storeDir, StringLogger stringLogger, StoreIdGetter storeIdGetter, Config conf,
            SlaveDatabaseOperations localDatabase, ClusterEventReceiver clusterReceiver )
    {
        super( conf.get( HaSettings.coordinators ),
            storeIdGetter, stringLogger,
            conf.getInteger( read_timeout ),
            conf.isSet( lock_read_timeout ) ? conf.getInteger( lock_read_timeout) : conf.getInteger( read_timeout ),
            conf.getInteger( max_concurrent_channels_per_slave ),
            conf.getInteger( zk_session_timeout ));
        this.storeDir = storeDir;
        this.conf = conf;
        this.localDatabase = localDatabase;
        this.clusterReceiver = clusterReceiver;
        machineId = conf.getInteger( server_id );
        backupPort = conf.getInteger( OnlineBackupSettings.online_backup_port);
        haServer = conf.isSet(server) ? conf.get( server ) : defaultServer();
        writeLastCommittedTx = conf.getEnum(SlaveUpdateMode.class, slave_coordinator_update_mode).syncWithZooKeeper;
        clusterName = conf.get( cluster_name );
        sequenceNr = "not initialized yet";
        allowCreateCluster = conf.getBoolean( allow_init_cluster );

        try
        {
            zooKeeper = new ZooKeeper( getServers(), getSessionTimeout(), new WatcherImpl() );
        }
        catch ( IOException e )
        {
            throw new ZooKeeperException(
                "Unable to create zoo keeper client", e );
        }
    }

    private String defaultServer()
    {
        InetAddress host = null;
        try
        {
            host = InetAddress.getLocalHost();
        }
        catch ( UnknownHostException hostBecomesNull )
        {
            // handled by null check
        }
        if ( host == null )
        {
            throw new IllegalStateException(
                    "Could not auto configure host name, please supply " + HaSettings.server.name() );
        }
        return host.getHostAddress() + ":" + HaConfig.CONFIG_DEFAULT_PORT;
    }

    public Object instantiateMasterServer( GraphDatabaseAPI graphDb )
    {
        int timeOut = conf.isSet( lock_read_timeout ) ? conf.getInteger( lock_read_timeout ) : conf.getInteger( read_timeout );
        return new MasterServer( new MasterImpl( graphDb, timeOut ),
                Machine.splitIpAndPort( haServer ).other(), graphDb.getMessageLog(),
                conf.getInteger( max_concurrent_channels_per_slave ),
                clientLockReadTimeout, new BranchDetectingTxVerifier( graphDb ) );
    }

    @Override
    protected int getMyMachineId()
    {
        return this.machineId;
    }

    private int toInt( byte[] data )
    {
        return ByteBuffer.wrap( data ).getInt();
    }

    @Override
    void waitForSyncConnected( WaitMode mode )
    {
        if ( keeperState == KeeperState.SyncConnected )
        {
            return;
        }
        if ( shutdown == true )
        {
            throw new ZooKeeperException( "ZooKeeper client has been shutdwon" );
        }
        WaitStrategy strategy = mode.getStrategy( this );
        long startTime = System.currentTimeMillis();
        long currentTime = startTime;
        synchronized ( keeperStateMonitor )
        {
            do
            {
                try
                {
                    keeperStateMonitor.wait( 250 );
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
            while ( strategy.waitMore( ( currentTime - startTime ) ) );

            if ( keeperState != KeeperState.SyncConnected )
            {
                throw new ZooKeeperTimedOutException( "Connection to ZooKeeper server timed out, keeper state="
                                                      + keeperState );
            }
        }
    }

    protected void subscribeToDataChangeWatcher( String child )
    {
        String root = getRoot();
        String path = root + "/" + child;
        try
        {
            try
            {
                zooKeeper.getData( path, true, null );
            }
            catch ( KeeperException e )
            {
                if ( e.code() == KeeperException.Code.NONODE )
                {   // Create it if it doesn't exist
                    byte[] data = new byte[4];
                    ByteBuffer.wrap( data ).putInt( -1 );
                    try
                    {
                        zooKeeper.create( path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
                    }
                    catch ( KeeperException ce )
                    {
                        if ( e.code() != KeeperException.Code.NODEEXISTS ) throw new ZooKeeperException( "Creation error", ce );
                    }
                }
                else throw new ZooKeeperException( "Couldn't get or create " + child, e );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Interrupted", e );
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

                if ( ByteBuffer.wrap( data ).getInt() == currentMasterId )
                {
                    msgLog.logMessage( child + " not set, is already " + currentMasterId );
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

    @Override
    public String getRoot()
    {
        makeSureRootPathIsFound();

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

    private void makeSureRootPathIsFound()
    {
        if ( rootPath == null )
        {
            storeId = getClusterStoreId( zooKeeper, clusterName );
            if ( storeId != null )
            {   // There's a cluster in place, let's use that
                rootPath = asRootPath( storeId );
                if ( NeoStoreUtil.storeExists( storeDir ) )
                {   // We have a local store, use and verify against it
                    NeoStoreUtil store = new NeoStoreUtil( storeDir );
                    committedTx = store.getLastCommittedTx();
                    if ( !storeId.equals( store.asStoreId() ) ) throw new ZooKeeperException( "StoreId in database doesn't match that of the ZK cluster" );
                }
                else
                {   // No local store
                    committedTx = 1;
                }
            }
            else
            {   // Cluster doesn't exist
                if ( !allowCreateCluster ) throw new RuntimeException( "Not allowed to create cluster" );
                StoreId storeIdSuggestion = NeoStoreUtil.storeExists( storeDir ) ?
                        new NeoStoreUtil( storeDir ).asStoreId() : new StoreId();
                storeId = createCluster( storeIdSuggestion );
                makeSureRootPathIsFound();
            }
            masterForCommittedTx = getFirstMasterForTx( committedTx );
        }
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

    private byte[] dataRepresentingMe( long txId, int master )
    {
        byte[] array = new byte[12];
        ByteBuffer buffer = ByteBuffer.wrap( array );
        buffer.putLong( txId );
        buffer.putInt( master );
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
            String created = zooKeeper.create( path, dataRepresentingMe( committedTx, masterForCommittedTx ),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );

            // Add watches to our master notification nodes
            subscribeToDataChangeWatcher( MASTER_NOTIFY_CHILD );
            subscribeToDataChangeWatcher( MASTER_REBOUND_CHILD );
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
        byte[] array = new byte[haServer.length()*2 + 100];
        ByteBuffer buffer = ByteBuffer.wrap( array );
        buffer.putInt( backupPort );
        buffer.put( (byte) haServer.length() );
        buffer.asCharBuffer().put( haServer.toCharArray() ).flip();
        byte[] actualArray = new byte[buffer.limit()];
        System.arraycopy( array, 0, actualArray, 0, actualArray.length );
        return actualArray;
    }

    public synchronized void setJmxConnectionData( JMXServiceURL jmxUrl, String instanceId )
    {
        String path = rootPath + "/" + HA_SERVERS_CHILD + "/" + machineId + "-jmx";
        String url = jmxUrl.toString();
        byte[] data = new byte[( url.length() + instanceId.length() ) * 2 + 4];
        ByteBuffer buffer = ByteBuffer.wrap( data );
        // write URL
        buffer.putShort( (short) url.length() );
        buffer.asCharBuffer().put( url.toCharArray() );
        buffer.position( buffer.position() + url.length() * 2 );
        // write instanceId
        buffer.putShort( (short) instanceId.length() );
        buffer.asCharBuffer().put( instanceId.toCharArray() );
        // truncate array
        if ( buffer.limit() != data.length )
        {
            byte[] array = new byte[buffer.limit()];
            System.arraycopy( data, 0, array, 0, array.length );
            data = array;
        }
        try
        {
            try
            {
                zooKeeper.setData( path, data, -1 );
            }
            catch ( KeeperException e )
            {
                if ( e.code() == KeeperException.Code.NONODE )
                {
                    zooKeeper.create( path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL );
                }
                else
                {
                    msgLog.logMessage( "Unable to set jxm connection info", e );
                }
            }
        }
        catch ( KeeperException e )
        {
            msgLog.logMessage( "Unable to set jxm connection info", e );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            msgLog.logMessage( "Unable to set jxm connection info", e );
        }
    }

    public void getJmxConnectionData( ConnectionInformation connection )
    {
        String path = rootPath + "/" + HA_SERVERS_CHILD + "/" + machineId + "-jmx";
        byte[] data;
        try
        {
            data = zooKeeper.getData( path, false, null );
        }
        catch ( KeeperException e )
        {
            return;
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            return;
        }
        if ( data == null || data.length == 0 ) return;
        ByteBuffer buffer = ByteBuffer.wrap( data );
        char[] url, instanceId;
        try
        {
            // read URL
            url = new char[buffer.getShort()];
            buffer.asCharBuffer().get( url );
            buffer.position( buffer.position() + url.length * 2 );
            // read instanceId
            instanceId = new char[buffer.getShort()];
            buffer.asCharBuffer().get( instanceId );
        }
        catch ( BufferUnderflowException e )
        {
            return;
        }
        connection.setJMXConnectionData( new String( url ), new String( instanceId ) );
    }

    public synchronized void setCommittedTx( long tx )
    {
        waitForSyncConnected();
        this.committedTx = tx;
        int master = localDatabase.getMasterForTx( tx );
        this.masterForCommittedTx = master;
        String root = getRoot();
        String path = root + "/" + machineId + "_" + sequenceNr;
        byte[] data = dataRepresentingMe( tx, master );
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

    private int getFirstMasterForTx( long committedTx )
    {
        if ( committedTx == 1 ) return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
        LogExtractor extractor = null;
        try
        {
            extractor = LogExtractor.from( storeDir, committedTx );
            long tx = extractor.extractNext( NullLogBuffer.INSTANCE );
            if ( tx != committedTx )
            {
                msgLog.logMessage( "Tried to extract master for tx " + committedTx + " at initialization, but got tx " + tx +
                        " back. Will be using " + XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER + " temporarily" );
                return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
            }
            return extractor.getLastStartEntry().getMasterId();
        }
        catch ( IOException e )
        {
            msgLog.logMessage( "Couldn't get master for " + committedTx + " using " +
                    XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER + " temporarily", e );
            return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
        }
        finally
        {
            if ( extractor != null ) extractor.close();
        }
    }

    @Override
    public void shutdown()
    {
        this.shutdown = true;
        super.shutdown();
    }

    public boolean isShutdown()
    {
        return shutdown;
    }

    @Override
    public ZooKeeper getZooKeeper( boolean sync )
    {
        if ( sync ) zooKeeper.sync( rootPath, null, null );
        return zooKeeper;
    }

    @Override
    protected String getHaServer( int machineId, boolean wait )
    {
        return machineId == this.machineId ? haServer : super.getHaServer( machineId, wait );
    }

    private synchronized StoreId createCluster( StoreId storeIdSuggestion )
    {
        String path = "/" + clusterName;
        try
        {
            try
            {
                zooKeeper.create( path, storeIdSuggestion.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT );
                return storeIdSuggestion; // if successfully written
            }
            catch ( KeeperException e )
            {
                if ( e.code() == KeeperException.Code.NODEEXISTS )
                { // another instance wrote before me
                    try
                    { // read what that instance wrote
                        return StoreId.deserialize( zooKeeper.getData( path, false, null ) );
                    }
                    catch ( KeeperException ex )
                    {
                        throw new ZooKeeperException( "Unable to read cluster store id", ex );
                    }
                }
                else
                {
                    throw new ZooKeeperException( "Unable to write cluster store id", e );
                }
            }
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( "createCluster interrupted", e );
        }
    }

    public StoreId getClusterStoreId( WaitMode mode )
    {
        waitForSyncConnected( mode );
        makeSureRootPathIsFound();
        return storeId;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[serverId:" + machineId + ", seq:" + sequenceNr +
                ", lastCommittedTx:" + committedTx + " w/ master:" + masterForCommittedTx +
                ", session:" + sessionId + "]";
    }

    private class WatcherImpl
            implements Watcher
    {
        public void process( WatchedEvent event )
        {
            try
            {
                String path = event.getPath();
                msgLog.logMessage( this + ", " + new Date() + " Got event: " + event + " (path=" + path + ")", true );
                if ( path == null && event.getState() == Watcher.Event.KeeperState.Expired )
                {
                    keeperState = KeeperState.Expired;
                    clusterReceiver.reconnect( new Exception() );
                }
                else if ( path == null && event.getState() == Watcher.Event.KeeperState.SyncConnected )
                {
                    long newSessionId = zooKeeper.getSessionId();
                    Pair<Master, Machine> masterBeforeIWrite = getMasterFromZooKeeper( false, false );
                    msgLog.logMessage( "Get master before write:" + masterBeforeIWrite );
                    boolean masterBeforeIWriteDiffers = masterBeforeIWrite.other().getMachineId() != getCachedMaster().other().getMachineId();
                    if ( newSessionId != sessionId || masterBeforeIWriteDiffers )
                    {
                        if ( writeLastCommittedTx )
                        {
                            sequenceNr = setup();
                            msgLog.logMessage( "Did setup, seq=" + sequenceNr + " new sessionId=" + newSessionId );
                            Pair<Master, Machine> masterAfterIWrote = getMasterFromZooKeeper( false, false );
                            msgLog.logMessage( "Get master after write:" + masterAfterIWrote );
                            if ( sessionId != -1 )
                            {
                                clusterReceiver.newMaster( new Exception( "Got SyncConnected event from ZK" ) );
                            }
                            sessionId = newSessionId;
                        }
                        else
                        {
                            msgLog.logMessage( "Didn't do setup due to told not to write" );
                            keeperState = KeeperState.SyncConnected;
                            subscribeToDataChangeWatcher( MASTER_REBOUND_CHILD );
                        }
                        keeperState = KeeperState.SyncConnected;
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
                else if ( event.getType() == Watcher.Event.EventType.NodeDeleted )
                {
                    msgLog.logMessage( "Got a NodeDeleted event for " + path );
                    ZooKeeperMachine currentMaster = (ZooKeeperMachine) getCachedMaster().other();
                    if ( path.contains( currentMaster.getZooKeeperPath() ) )
                    {
                        msgLog.logMessage("Acting on it, calling newMaster()");
                        clusterReceiver.newMaster( new Exception() );
                    }
                }
                else if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
                {
                    int newMasterMachineId = toInt( getZooKeeper( true ).getData( path, true, null ) );
                    msgLog.logMessage( "Got event data " + newMasterMachineId );
                    if ( path.contains( MASTER_NOTIFY_CHILD ) )
                    {
                        // This event is for the masters eyes only so it should only
                        // be the (by zookeeper spoken) master which should make sure
                        // it really is master.
                        if ( newMasterMachineId == machineId )
                        {
                            clusterReceiver.newMaster( new Exception() );
                        }
                    }
                    else if ( path.contains( MASTER_REBOUND_CHILD ) )
                    {
                        // This event is for all the others after the master got the
                        // MASTER_NOTIFY_CHILD which then shouts out to the others to
                        // become slaves if they don't already are.
                        if ( newMasterMachineId != machineId )
                        {
                            clusterReceiver.newMaster( new Exception() );
                        }
                    }
                    else
                    {
                        msgLog.logMessage( "Unrecognized data change " + path );
                    }
                }
            }
            catch ( Exception e )
            {
                msgLog.logMessage( "Error in ZooClient.process", e, true );
                e.printStackTrace();
                throw Exceptions.launderedException( e );
            }
                finally
            {
                    msgLog.flush();
            }
        }
    }
}
