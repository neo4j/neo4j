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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.remote.JMXServiceURL;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.InformativeStackTrace;
import org.neo4j.kernel.SlaveUpdateMode;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterEventReceiver;
import org.neo4j.kernel.ha.ConnectionInformation;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.MasterClientFactory;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.Slave;
import org.neo4j.kernel.ha.SlaveClient;
import org.neo4j.kernel.ha.SlaveDatabaseOperations;
import org.neo4j.kernel.ha.SlaveImpl;
import org.neo4j.kernel.ha.SlaveServer;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.NullLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.StringLogger;

public class ZooClient extends AbstractZooKeeperManager
{
    static final String MASTER_NOTIFY_CHILD = "master-notify";
    static final String MASTER_REBOUND_CHILD = "master-rebound";

    private final ZooKeeper zooKeeper;
    private final int machineId;
    private String sequenceNr;

    private long committedTx;
    private int masterForCommittedTx;

    private final Object keeperStateMonitor = new Object();
    private volatile KeeperState keeperState = KeeperState.Disconnected;
    private volatile boolean shutdown = false;
    private volatile boolean flushing = false;
    private String rootPath;
    private volatile StoreId storeId;
    private volatile TxIdUpdater updater = new NoUpdateTxIdUpdater();

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
    private final WatcherImpl watcher;

    private final Machine asMachine;

    private final Map<Integer, Pair<SlaveClient,Machine>> cachedSlaves = new HashMap<Integer, Pair<SlaveClient,Machine>>();
    private volatile boolean serversRefreshed = true;

    public ZooClient( String storeDir, StringLogger stringLogger, Config conf, SlaveDatabaseOperations localDatabase,
            ClusterEventReceiver clusterReceiver, MasterClientFactory clientFactory )
    {
        super( conf.get( HaSettings.coordinators ), stringLogger, conf.getInteger( zk_session_timeout ), clientFactory );
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
        asMachine = new Machine( machineId, 0, 0, 0, haServer, backupPort );

        try
        {
            /*
             * Chicken and egg problem of sorts. The WatcherImpl might need zooKeeper instance
             * before its constructor has returned, hence the "flushUnprocessedEvents" workaround.
             * Without it the watcher might throw NPE on the initial (maybe SyncConnected) events,
             * which would effectively prevent a client from feeling connected to its ZK cluster.
             * See WatcherImpl for more detail on this.
             */
            watcher = new WatcherImpl();
            zooKeeper = new ZooKeeper( getServers(), getSessionTimeout(), watcher );
            watcher.flushUnprocessedEvents( zooKeeper );
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
        return new MasterServer( new MasterImpl( graphDb, timeOut ), Machine.splitIpAndPort( haServer ).other(),
                graphDb.getMessageLog(), conf.getInteger( max_concurrent_channels_per_slave ), timeOut,
                new BranchDetectingTxVerifier( graphDb ) );
    }

    @Override
    protected StoreId getStoreId()
    {
        return storeId;
    }

    public Object instantiateSlaveServer( GraphDatabaseAPI graphDb, Broker broker, SlaveDatabaseOperations ops )
    {
        return new SlaveServer( new SlaveImpl( graphDb, broker, ops ), Machine.splitIpAndPort( haServer ).other(),
                graphDb.getMessageLog() );
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

    protected void subscribeToChildrenChangeWatcher( String child )
    {
        String path = getRoot() + "/" + child;
        try
        {
            zooKeeper.getChildren( path, true );
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Couldn't subscribe getChildren at " + path, e );
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
        throw new IllegalStateException( "Root path couldn't be found" );
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
            subscribeToChildrenChangeWatcher( HA_SERVERS_CHILD );
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
        String serverRootPath = rootPath + "/" + HA_SERVERS_CHILD;
        try
        {
            zooKeeper.create( serverRootPath, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
        }
        catch ( KeeperException e )
        {
            if ( e.code() != KeeperException.Code.NODEEXISTS )
            {
                throw e;
            }
        }

        // Make sure the compatibility node is present
        String compatibilityPath = rootPath + "/" + COMPATIBILITY_CHILD;
        try
        {
            zooKeeper.create( compatibilityPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
        }
        catch ( KeeperException e )
        {
            if ( e.code() != KeeperException.Code.NODEEXISTS )
            {
                throw e;
            }
        }

        // Write the HA server config.
        String machinePath = serverRootPath + "/" + machineId;
        String compatibilityMachinePath = compatibilityPath + "/" + machineId;
        byte[] data = haServerAsData();
        boolean compatCreated = false;
        boolean machineCreated = false;
        try
        {
            zooKeeper.create( compatibilityMachinePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL );
            compatCreated = true;
            zooKeeper.create( machinePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL );
            machineCreated = true;
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
                if ( compatCreated ) zooKeeper.delete( compatibilityMachinePath, -1 );
                if ( machineCreated ) zooKeeper.delete( machinePath, -1 );
            }
            catch ( KeeperException ee )
            {
                if ( ee.code() != KeeperException.Code.NONODE )
                {
                    msgLog.logMessage( "Unable to delete " + ee.getPath(), ee );
                }
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

    /*
     * Start/stop flushing infrastructure follows. When master goes down
     * all machines will gang up and try to elect master. So we will get
     * many "start flushing" events but we only need respect one. The same
     * goes for stop flushing events. For that reason we first do a quick
     * check if we are already in the state we are trying to reach and
     * bail out quickly. Otherwise we just grab the lock and
     * synchronously update. The checking outside the lock is the
     * reason the flushing boolean flag is volatile.
     */

    private void startFlushing()
    {
        if ( checkCompatibilityMode() )
        {
            msgLog.logMessage( "Discovered compatibility node, will remain in compatibility mode until the node is removed" );
            updater = new CompatibilitySlaveOnlyTxIdUpdater();
            updater.init();
        }
        else if ( !flushing )
        {
            synchronized ( this )
            {
                if ( !flushing )
                {
                    flushing = true;
                    updater = new SynchronousTxIdUpdater();
                    updater.init();
                }
            }
        }
    }

    private void stopFlushing()
    {
        if ( checkCompatibilityMode() )
        {
            msgLog.logMessage( "Discovered compatibility node, will remain in compatibility mode until the node is removed" );
            updater = new CompatibilitySlaveOnlyTxIdUpdater();
            updater.init();
        }
        else if ( flushing )
        {
            synchronized ( this )
            {
                if ( flushing )
                {
                    flushing = false;
                    updater = new NoUpdateTxIdUpdater();
                    updater.init();
                }
            }
        }
    }

    public synchronized void setCommittedTx( long tx )
    {
        this.committedTx = tx;
        updater.updatedTxId( tx );
    }

    private void writeData( long tx, int masterForThat )
    {
        waitForSyncConnected();
        String root = getRoot();
        String path = root + "/" + machineId + "_" + sequenceNr;
        byte[] data = dataRepresentingMe( tx, masterForThat );
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
        watcher.shutdown();
        msgLog.close();
        this.shutdown = true;
        shutdownSlaves();
        super.shutdown();
    }

    private void shutdownSlaves()
    {
        for ( Pair<SlaveClient,Machine> slave : cachedSlaves.values() )
            slave.first().shutdown();
        cachedSlaves.clear();
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
    protected Machine getHaServer( int machineId, boolean wait )
    {
        return machineId == this.machineId ? asMachine : super.getHaServer( machineId, wait );
    }

    private boolean checkCompatibilityMode()
    {
        try
        {
            refreshHaServers();
            int totalCount = getNumberOfServers();
            int myVersionCount = zooKeeper.getChildren( getRoot() + "/" + COMPATIBILITY_CHILD, false ).size();
            boolean result = myVersionCount <= totalCount - 1;
            msgLog.logMessage( "Checking compatibility mode, read " + totalCount + " as all machines, "
                               + myVersionCount + " as myVersion machines. Based on that I return " + result );
            return result;

        }
        catch ( Exception e )
        {
            msgLog.logMessage( "Tried to discover if we are in compatibility mode, got this exception instead", e );
            throw new RuntimeException( e );
        }
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
    protected String getSequenceNr()
    {
        return sequenceNr;
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
        private final Queue<WatchedEvent> unprocessedEvents = new LinkedList<WatchedEvent>();
        private final ExecutorService threadPool = Executors.newCachedThreadPool();
        private final AtomicInteger count = new AtomicInteger( 0 );
        private volatile boolean electionHappening = false;

        /**
         * Flush all events we got before initialization of
         * ZooKeeper/WatcherImpl was completed.
         *
         * @param zooKeeper ZooKeeper instance to use when processing. We cannot
         *            rely on the
         *            zooKeeper instance inherited from ZooClient since the
         *            contract says that such fields
         *            are only guaranteed to be published when the constructor
         *            has returned, and at this
         *            point it hasn't.
         */
        protected void flushUnprocessedEvents( ZooKeeper zooKeeper )
        {
            synchronized ( unprocessedEvents )
            {
                WatchedEvent e = null;
                while ( (e = unprocessedEvents.poll()) != null )
                    runEventInThread( e, zooKeeper );
            }
        }

        public void shutdown()
        {
            threadPool.shutdown();
            try
            {
                threadPool.awaitTermination( 10, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                msgLog.logMessage( ZooClient.this + " couldn't flush pending events in time during shutdown", true );
            }
        }

        public void process( final WatchedEvent event )
        {
            /*
             * MP: The setup we have here is messed up. Why do I say that? Well, we've got
             * this watcher which uses the ZooKeeper object that it's set to watch. And
             * it is passed in to the constructor of the ZooKeeper object. So, if this watcher
             * gets an event before the ZooKeeper constructor returns we're screwed here.
             *
             * Cue unprocessedEvents queue. It will act as a shield for this design blunder
             * and absorb the events we get before everything is properly initialized,
             * and emit them right thereafter (see #flushUnprocessedEvents()).
             */
            synchronized ( unprocessedEvents )
            {
                if ( zooKeeper == null || !unprocessedEvents.isEmpty() )
                {
                    unprocessedEvents.add( event );
                    return;
                }
            }
            runEventInThread( event, zooKeeper );
        }

        private synchronized void runEventInThread( final WatchedEvent event, final ZooKeeper zoo )
        {
            if ( shutdown )
            {
                return;
            }
            if ( count.get() > 10 )
            {
                msgLog.logMessage( "Thread count is already at " + count.get()
                                   + " and added another ZK event handler thread." );
            }
            threadPool.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    processEvent( event, zoo );
                }
            } );
        }

        private void processEvent( WatchedEvent event, ZooKeeper zooKeeper )
        {
            try
            {
                count.incrementAndGet();
                String path = event.getPath();
                msgLog.logMessage( this + ", " + new Date() + " Got event: " + event + " (path=" + path + ")", true );
                if ( path == null && event.getState() == Watcher.Event.KeeperState.Expired )
                {
                    keeperState = KeeperState.Expired;
                    clusterReceiver.reconnect( new InformativeStackTrace( "Reconnect due to session expired" ) );
                }
                else if ( path == null && event.getState() == Watcher.Event.KeeperState.SyncConnected )
                {
                    long newSessionId = zooKeeper.getSessionId();
                    if ( newSessionId != sessionId )
                    {
                        if ( writeLastCommittedTx )
                        {
                            sequenceNr = setup();
                            msgLog.logMessage( "Did setup, seq=" + sequenceNr + " new sessionId=" + newSessionId );
                            if ( sessionId != -1 )
                            {
                                clusterReceiver.newMaster( new InformativeStackTrace( "Got SyncConnected event from ZK" ) );
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
                        if ( checkCompatibilityMode() )
                        {
                            msgLog.logMessage( "Discovered compatibility node, will remain in compatibility mode until the node is removed" );
                            updater = new CompatibilitySlaveOnlyTxIdUpdater();
                        }
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
                                clusterReceiver.newMaster( new InformativeStackTrace(
                                        "NodeDeleted event received (a machine left the cluster)" ) );
                    }
                }
                else if ( event.getType() == Watcher.Event.EventType.NodeChildrenChanged )
                {
                    if ( path.endsWith( HA_SERVERS_CHILD ) )
                    {
                        try
                        {
                            refreshHaServers();
                            serversRefreshed = true;
                            subscribeToChildrenChangeWatcher( HA_SERVERS_CHILD );
                        }
                        catch ( ZooKeeperException e )
                        {
                            // Happens for session expiration, why?
                        }
                    }
                }
                else if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
                {
                    int updatedData = toInt( getZooKeeper( true ).getData( path, true, null ) );
                    msgLog.logMessage( "Got event data " + updatedData );
                    if ( path.contains( MASTER_NOTIFY_CHILD ) )
                    {
                        // This event is for the masters eyes only so it should only
                        // be the (by zookeeper spoken) master which should make sure
                        // it really is master.
                        if ( updatedData == machineId && !electionHappening )
                        {
                            try
                            {
                                electionHappening = true;
                                clusterReceiver.newMaster( new InformativeStackTrace(
                                        "NodeDataChanged event received (someone though I should be the master)" ) );
                                serversRefreshed = true;
                            }
                            finally
                            {
                                electionHappening = false;
                            }
                        }
                    }
                    else if ( path.contains( MASTER_REBOUND_CHILD ) )
                    {
                        // This event is for all the others after the master got the
                        // MASTER_NOTIFY_CHILD which then shouts out to the others to
                        // become slaves if they don't already are.
                        if ( updatedData != machineId && !electionHappening )
                        {
                            try
                            {
                                electionHappening = true;
                                clusterReceiver.newMaster( new InformativeStackTrace(
                                        "NodeDataChanged event received (new master ensures I'm slave)" ) );
                                serversRefreshed = true;
                            }
                            finally
                            {
                                electionHappening = false;
                            }
                        }
                    }
                    else if ( path.contains( FLUSH_REQUESTED_CHILD ) )
                    {
                        if ( updatedData == STOP_FLUSHING )
                        {
                            stopFlushing();
                        }
                        else
                        {
                            startFlushing();
                        }
                    }
                    else
                    {
                        msgLog.logMessage( "Unrecognized data change " + path );
                    }
                }
            }
            catch ( BrokerShutDownException e )
            {
                // It's OK. We're in a state where we cannot accept incoming events.
            }
            catch ( Exception e )
            {
                msgLog.logMessage( "Error in ZooClient.process", e, true );
                throw Exceptions.launderedException( e );
            }
            finally
            {
                msgLog.flush();
                count.decrementAndGet();
            }
        }
    }

    private abstract class AbstractTxIdUpdater implements TxIdUpdater
    {
        @Override
        public void updatedTxId( long txId )
        {
            // default no op
        }
    }

    private class SynchronousTxIdUpdater extends AbstractTxIdUpdater
    {
        public void init()
        {
            /*
             * this is safe because we call getFirstMasterForTx() on the same
             * txid that we post - and we are supposed to be called from setCommitedTxId()
             * which is synchronized, which means we can't have committedTx changed while
             * we are here.
             */
            writeData( committedTx, getFirstMasterForTx( committedTx ) );
            msgLog.logMessage( "Starting flushing of txids to zk, while at txid " + committedTx );
        }

        @Override
        public void updatedTxId( long txId )
        {
            masterForCommittedTx = localDatabase.getMasterForTx( txId );
            writeData( txId, masterForCommittedTx );
        }
    }

    private class NoUpdateTxIdUpdater extends AbstractTxIdUpdater
    {
        @Override
        public void init()
        {
            writeData( -2, -2 );
            msgLog.logMessage( "Stopping flushing of txids to zk, while at txid " + committedTx );
        }
    }

    private class CompatibilitySlaveOnlyTxIdUpdater extends AbstractTxIdUpdater
    {
        public void init()
        {
            writeData( 0, -1 );
            msgLog.logMessage( "Set to defaults (0 for txid, -1 for master) since we are running in compatilibility mode, while at txid "
                               + committedTx );
        }
    }

    @Override
    protected void invalidateMaster()
    {
        super.invalidateMaster();
        serversRefreshed = true;
    }

    public Slave[] getSlavesFromZooKeeper()
    {
        synchronized ( cachedSlaves )
        {
            if ( serversRefreshed || cachedSlaves.isEmpty() )
            {
                // Go through the cached list and refresh where needed
                Machine master = cachedMaster.other();
                if ( master.getMachineId() == this.machineId )
                {   // I'm the master, update/populate the slave list
                    Set<Integer> visitedSlaves = new HashSet<Integer>();
                    for ( Machine machine : getHaServers() )
                    {
                        int id = machine.getMachineId();
                        visitedSlaves.add( id );
                        if ( id == machineId )
                            continue;

                        boolean instantiate = true;
                        Pair<SlaveClient, Machine> existingSlave = cachedSlaves.get( id );
                        if ( existingSlave != null )
                        {   // We already have a cached slave for this machine, check if
                            // it's the same server information
                            Machine existingMachine = existingSlave.other();
                            if ( existingMachine.getServer().equals( machine.getServer() ) )
                                instantiate = false;
                            else
                                // Connection information changed, needs refresh
                                existingSlave.first().shutdown();
                        }

                        if ( instantiate )
                        {
                            cachedSlaves.put( id, Pair.of( new SlaveClient( machine.getMachineId(), machine.getServer().first(),
                                    machine.getServer().other().intValue(), msgLog, storeId,
                                    conf.get( HaSettings.max_concurrent_channels_per_slave ) ),
                                    machine ) );
                        }
                    }

                    Integer[] existingSlaves = cachedSlaves.keySet().toArray( new Integer[cachedSlaves.size()] );
                    for ( int id : existingSlaves )
                        if ( !visitedSlaves.contains( id ) )
                            cachedSlaves.remove( id ).first().shutdown();
                }
                else
                {   // I'm a slave, I don't need a slave list so clear any existing
                    shutdownSlaves();
                }
                serversRefreshed = true;
            }

            Slave[] slaves = new Slave[cachedSlaves.size()];
            int i = 0;
            for ( Pair<SlaveClient, Machine> slave : cachedSlaves.values() )
                slaves[i++] = slave.first();
            return slaves;
        }
    }
}
