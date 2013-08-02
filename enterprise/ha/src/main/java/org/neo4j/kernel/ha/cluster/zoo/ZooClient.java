/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.zoo;
/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.NeoStoreUtil;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.switchover.CompatibilityModeListener;
import org.neo4j.kernel.ha.switchover.CompatibilityMonitor;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.NullLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.cluster.ClusterSettings.server_id;

public class ZooClient implements Lifecycle, CompatibilityMonitor
{
    static final String MASTER_NOTIFY_CHILD = "master-notify";
    static final String MASTER_REBOUND_CHILD = "master-rebound";
    protected static final String HA_SERVERS_CHILD = "ha-servers";
    protected static final String FLUSH_REQUESTED_CHILD = "flush-requested";
    protected static final String COMPATIBILITY_CHILD_18 = "compatibility-1.8";
    protected static final String COMPATIBILITY_CHILD_19 = "compatibility-1.9";

    private ZooKeeper zooKeeper;
    private int machineId;
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
    private String clusterServer;

    private File storeDir;
    private long sessionId = -1;
    private HostnamePort backupPort;
    private String clusterName;
    private boolean allowCreateCluster;
    private WatcherImpl watcher;

    private Machine asMachine;
    private final Config conf;
    private Iterable<ZooListener> zooListeners = Listeners.newListeners();
    private Iterable<CompatibilityModeListener> compatibilityListeners = Listeners.newListeners();

    protected static final int STOP_FLUSHING = -6;

    private List<HostnamePort> servers;
    private final Map<Integer, Machine> haServersCache = new ConcurrentHashMap<Integer, Machine>();
    protected volatile Machine cachedMaster = NO_MACHINE;

    protected final StringLogger msgLog;
    private long sessionTimeout;
    private String haServer;
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    public ZooClient( StringLogger stringLogger, Config conf )
    {
        this.conf = conf;
        this.msgLog = stringLogger;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        this.storeDir = conf.get( GraphDatabaseSettings.store_dir );
        machineId = conf.get( server_id );
        backupPort = conf.get( OnlineBackupSettings.online_backup_server );
        clusterServer = NetworkReceiver.URI_PROTOCOL + "://" +
                conf.get( ClusterSettings.cluster_server ).getHost( defaultServer() ) + ":" +
                conf.get( ClusterSettings.cluster_server ).getPort();
        haServer = "ha://" + conf.get( HaSettings.ha_server ).getHost( defaultServer() ) + ":" +
                conf.get( ClusterSettings.cluster_server ).getPort();
        clusterName = conf.get( ClusterSettings.cluster_name );
        allowCreateCluster = conf.get( ClusterSettings.allow_init_cluster );
        asMachine = new Machine( machineId, 0, 0, 0, haServer, backupPort.getPort() );
        this.servers = conf.get( HaSettings.coordinators );
        this.sessionTimeout = conf.get( HaSettings.zk_session_timeout );
        sequenceNr = "not initialized yet";

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
            zooKeeper = new ZooKeeper( getServersAsString(), getSessionTimeout(), watcher );
            watcher.flushUnprocessedEvents( zooKeeper );
        }
        catch ( IOException e )
        {
            throw new ZooKeeperException(
                    "Unable to create zoo keeper client", e );
        }
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown()
    {
        try
        {
            watcher.shutdown();
            this.shutdown = true;
            invalidateMaster();
            cachedMaster = NO_MACHINE;
            getZooKeeper( false ).close();
            msgLog.logMessage( "zoo client shut down" );
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException(
                    "Error closing zookeeper connection", e );
        }
    }


    /**
     * Tries to discover the master from the zookeeper information. Will return
     * a {@link Pair} of a {@link Master} and the {@link Machine} it resides
     * on. If the new master is different than the current then the current is
     * invalidated and if allowChange is set to true then the a connection to
     * the new master is established otherwise a NO_MASTER is returned.
     *
     * @return The master URI as a String
     */
    public String refreshMasterFromZooKeeper()
    {
        return getMasterFromZooKeeper( true, WaitMode.SESSION, true );
    }

    private String getMasterFromZooKeeper( boolean wait, WaitMode mode, boolean allowChange )
    {
        ZooKeeperMachine master = getMasterBasedOn( getAllMachines( wait, mode ).values() );
        masterElectionHappened( cachedMaster, master );
        if ( cachedMaster.getMachineId() != master.getMachineId() )
        {
            invalidateMaster();
            if ( !allowChange )
            {
                return "";
            }
            cachedMaster = master;
        }
        return cachedMaster.getServerAsString();
    }

    protected void invalidateMaster()
    {
        if ( cachedMaster != null )
        {
            cachedMaster = NO_MACHINE;
        }
    }

    public Machine getCachedMaster()
    {
        return cachedMaster;
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
                    "Could not auto configure host name, please supply " + HaSettings.ha_server.name() );
        }
        return host.getHostAddress();
    }

    protected StoreId getStoreId()
    {
        return storeId;
    }

    protected int getMyMachineId()
    {
        return this.machineId;
    }

    private int toInt( byte[] data )
    {
        return ByteBuffer.wrap( data ).getInt();
    }

    void waitForSyncConnected( WaitMode mode )
    {
        if ( keeperState == KeeperState.SyncConnected )
        {
            return;
        }
        if ( shutdown )
        {
            throw new ZooKeeperException( "ZooKeeper client has been shutdown" );
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
                if ( shutdown )
                {
                    throw new ZooKeeperException( "ZooKeeper client has been shutdown" );
                }
                currentTime = System.currentTimeMillis();
            }
            while ( strategy.waitMore( currentTime - startTime ) );

            if ( keeperState != KeeperState.SyncConnected )
            {
                throw new ZooKeeperException( "Connection to ZooKeeper server timed out, keeper state="
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
                        if ( e.code() != KeeperException.Code.NODEEXISTS )
                        {
                            throw new ZooKeeperException( "Creation error", ce );
                        }
                    }
                }
                else
                {
                    throw new ZooKeeperException( "Couldn't get or create " + child, e );
                }
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
        setDataChangeWatcher( child, currentMasterId, true );
    }

    /**
     * Writes into one of master-notify or master-rebound the value given. If skipOnSame
     * is true then if the value there is the same as the argument nothing will be written.
     * A watch is always set on the node.
     *
     * @param child           The node to write to
     * @param currentMasterId The value to write
     * @param skipOnSame      If true, then if the existing value is the same as currentMasterId nothing
     *                        will be written.
     */
    protected void setDataChangeWatcher( String child, int currentMasterId, boolean skipOnSame )
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

                if ( skipOnSame && ByteBuffer.wrap( data ).getInt() == currentMasterId )
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
                    NeoStoreUtil store = new NeoStoreUtil( storeDir);
                    committedTx = store.getLastCommittedTx();
                    if ( !storeId.equals( store.asStoreId() ) )
                    {
                        throw new ZooKeeperException( "StoreId in database doesn't match that of the ZK cluster" );
                    }
                }
                else
                {   // No local store
                    committedTx = 1;
                }
            }
            else
            {   // Cluster doesn't exist
                if ( !allowCreateCluster )
                {
                    throw new RuntimeException( "Not allowed to create cluster" );
                }
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
            subscribeToChildrenChangeWatcher( COMPATIBILITY_CHILD_19 );
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
        String legacyCompatibilityPath = rootPath + "/" + COMPATIBILITY_CHILD_18;
        /*
        try
        {
            zooKeeper.create( legacyCompatibilityPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT );
        }
        catch ( KeeperException e )
        {
            if ( e.code() != KeeperException.Code.NODEEXISTS )
            {
                throw e;
            }
        }
        */

        // Make sure the compatibility node is present
        String compatibilityPath = rootPath + "/" + COMPATIBILITY_CHILD_19;
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
        String legacyCompatibilityMachinePath = legacyCompatibilityPath + "/" + machineId;
        String compatibilityMachinePath = compatibilityPath + "/" + machineId;
        byte[] data = haServerAsData();
        boolean legacyCompatCreated = false;
        boolean compatCreated = false;
        boolean machineCreated = false;
        try
        {
            zooKeeper.create( legacyCompatibilityMachinePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL );
            legacyCompatCreated = true;
            zooKeeper.create( compatibilityMachinePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL );
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
            Thread.sleep(3000);
            try
            {
                if ( legacyCompatCreated )
                {
                    zooKeeper.delete( legacyCompatibilityMachinePath, -1 );
                }
                if ( compatCreated )
                {
                    zooKeeper.delete( compatibilityMachinePath, -1 );
                }
                if ( machineCreated )
                {
                    zooKeeper.delete( machinePath, -1 );
                }
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
        byte[] array = new byte[haServer.length() * 2 + 100];
        ByteBuffer buffer = ByteBuffer.wrap( array );
        buffer.putInt( backupPort.getPort() );
        buffer.put( (byte) haServer.substring( 5 ).length() );
        buffer.asCharBuffer().put( haServer.substring( 5 ).toCharArray() ).flip();
        byte[] actualArray = new byte[buffer.limit()];
        System.arraycopy( array, 0, actualArray, 0, actualArray.length );
        return actualArray;
    }

    public int getCurrentMasterNotify()
    {
        String path = rootPath + "/" + MASTER_NOTIFY_CHILD;
        byte[] data;
        try
        {
            data = zooKeeper.getData( path, true, null );
            return ByteBuffer.wrap( data ).getInt();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
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
        updater = new CompatibilitySlaveOnlyTxIdUpdater();
        updater.init();
    }

    private void stopFlushing()
    {
        updater = new CompatibilitySlaveOnlyTxIdUpdater();
        updater.init();
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
        if ( committedTx == 1 )
        {
            return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
        }
        LogExtractor extractor = null;
        try
        {
            extractor = LogExtractor.from( fileSystem, storeDir, committedTx );
            long tx = extractor.extractNext( NullLogBuffer.INSTANCE );
            if ( tx != committedTx )
            {
                msgLog.logMessage( "Tried to extract master for tx " + committedTx + " at " +
                        "initialization, but got tx " + tx +
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
            if ( extractor != null )
            {
                extractor.close();
            }
        }
    }

    protected void masterElectionHappened( Machine previousMaster, Machine newMaster )
    {
        if ( previousMaster == NO_MACHINE && newMaster.getMachineId() == getMyMachineId() )
        {
            setDataChangeWatcher( MASTER_REBOUND_CHILD, getMyMachineId(), false );
        }
    }

    public ZooKeeper getZooKeeper( boolean sync )
    {
        if ( sync )
        {
            zooKeeper.sync( rootPath, null, null );
        }
        return zooKeeper;
    }

    private boolean checkCompatibilityMode()
    {
        try
        {
            refreshHaServers();
            int totalCount = getNumberOfServers();
            int myVersionCount = zooKeeper.getChildren( getRoot() + "/" + COMPATIBILITY_CHILD_19, false ).size();
            boolean result = myVersionCount <= totalCount - 1;
            msgLog.logMessage( "Checking compatibility mode, " +
                    "read " + totalCount + " as all machines, "
                    + myVersionCount + " as myVersion machines. Based on that I return " + result );
            return result;
        }
        catch ( Exception e )
        {
            msgLog.logMessage( "Tried to discover if we are in compatibility mode, " +
                    "got this exception instead", e );
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


    protected StoreId getClusterStoreId( ZooKeeper keeper, String clusterName )
    {
        try
        {
            byte[] child = keeper.getData( "/" + clusterName, false, null );
            return StoreId.deserialize( child );
        }
        catch ( KeeperException e )
        {
            if ( e.code() == KeeperException.Code.NONODE )
            {
                return null;
            }
            throw new ZooKeeperException( "Error getting store id", e );
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( "Interrupted", e );
        }
    }


    protected String asRootPath( StoreId storeId )
    {
        return "/" + storeId.getCreationTime() + "_" + storeId.getRandomId();
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
        return Pair.of( id, seq );
    }

    protected Pair<Long, Integer> readDataRepresentingInstance( String path )
            throws InterruptedException, KeeperException
    {
        log( "reading data for instance " + path );
        byte[] data = getZooKeeper( false ).getData( path, false, null );
        ByteBuffer buf = ByteBuffer.wrap( data );
        return Pair.of( buf.getLong(), buf.getInt() );
    }

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

    public void addZooListener( ZooListener zooListener )
    {
        zooListeners = Listeners.addListener( zooListener, zooListeners );
    }

    @Override
    public void addCompatibilityModeListener( CompatibilityModeListener listener )
    {
        compatibilityListeners = Listeners.addListener( listener, compatibilityListeners );
    }

    @Override
    public void removeCompatibilityModeListener( CompatibilityModeListener listener )
    {
        compatibilityListeners = Listeners.removeListener( listener, compatibilityListeners );
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
         *                  rely on the
         *                  zooKeeper instance inherited from ZooClient since the
         *                  contract says that such fields
         *                  are only guaranteed to be published when the constructor
         *                  has returned, and at this
         *                  point it hasn't.
         */
        protected void flushUnprocessedEvents( ZooKeeper zooKeeper )
        {
            synchronized ( unprocessedEvents )
            {
                WatchedEvent e = null;
                while ( (e = unprocessedEvents.poll()) != null )
                {
                    runEventInThread( e, zooKeeper );
                }
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
                msgLog.logMessage( ZooClient.this + " couldn't flush pending events in time " +
                        "during shutdown", true );
            }
            log( "zoo watcher shut down" );
        }

        @Override
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
                msgLog.logMessage( this + ", " + new Date() + " Got event: " + event + " " +
                        "(path=" + path + ")", true );
                if ( path == null && event.getState() == Watcher.Event.KeeperState.Expired )
                {
                    keeperState = KeeperState.Expired;
                    Listeners.notifyListeners( zooListeners, new Listeners.Notification<ZooListener>()
                    {
                        @Override
                        public void notify( ZooListener listener )
                        {
                            listener.reconnect();
                        }
                    } );
                }
                else if ( path == null && event.getState() == Watcher.Event.KeeperState.SyncConnected )
                {
                    long newSessionId = zooKeeper.getSessionId();
                    if ( newSessionId != sessionId )
                    {
                        if ( true )
                        {
                            sequenceNr = setup();
                            msgLog.logMessage(
                                    "Did setup, seq=" + sequenceNr + " new sessionId=" + newSessionId );
                            int previousMaster = getCurrentMasterNotify(); // used in check below
                            if ( sessionId != -1 )
                            {
                                Listeners.notifyListeners( zooListeners, new Listeners.Notification<ZooListener>()
                                {
                                    @Override
                                    public void notify( ZooListener listener )
                                    {
                                        listener.newMasterRequired();
                                    }
                                } );
                                if ( getCurrentMasterNotify() == getMyMachineId() &&
                                        previousMaster == getMyMachineId() )
                                {
                                    /*
                                     * Apparently no one claimed the role of master while i was away.
                                     * I'll ping everyone to make sure.
                                     */
                                    setDataChangeWatcher( MASTER_REBOUND_CHILD, getMyMachineId(), false );
                                }
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
                            msgLog.logMessage(
                                    "Discovered compatibility node, will remain in compatibility mode until the node " +
                                            "is removed" );
                            updater = new CompatibilitySlaveOnlyTxIdUpdater();
                        }
                        else
                        {
                            msgLog.logMessage( "Was the only one in the cluster, restarting in Paxos mode" );
                            Listeners.notifyListeners( compatibilityListeners,
                                    new Listeners.Notification<CompatibilityModeListener>()
                                    {
                                        @Override
                                        public void notify( CompatibilityModeListener listener )
                                        {
                                            listener.leftCompatibilityMode();
                                        }
                                    } );
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
                    ZooKeeperMachine currentMaster = (ZooKeeperMachine) getCachedMaster();
                    if ( path.contains( currentMaster.getZooKeeperPath() ) && checkCompatibilityMode())
                    {
                        msgLog.logMessage( "Acting on it, calling newMaster()" );
                        Listeners.notifyListeners( zooListeners, new Listeners.Notification<ZooListener>()
                        {
                            @Override
                            public void notify( ZooListener listener )
                            {
                                listener.newMasterRequired();
                            }
                        } );
                    }
                }
                else if ( event.getType() == Watcher.Event.EventType.NodeChildrenChanged )
                {
                    if ( path.endsWith( HA_SERVERS_CHILD ) )
                    {
                        try
                        {
                            refreshHaServers();
                            subscribeToChildrenChangeWatcher( HA_SERVERS_CHILD );
                        }
                        catch ( ZooKeeperException e )
                        {
                            // Happens for session expiration, why?
                        }
                    }
                    else if ( path.endsWith( COMPATIBILITY_CHILD_19 ) )
                    {
                        msgLog.logMessage( "-> got compatibility event" );
                        subscribeToChildrenChangeWatcher( COMPATIBILITY_CHILD_19 );
                    }
                    if ( !checkCompatibilityMode() )
                    {
                        msgLog.logMessage( "No longer in compatibility mode, notifying listeners" );
                        Listeners.notifyListeners( compatibilityListeners,
                                new Listeners.Notification<CompatibilityModeListener>()
                                {
                                    @Override
                                    public void notify( CompatibilityModeListener listener )
                                    {
                                        listener.leftCompatibilityMode();
                                    }
                                } );
                    }
                }
                else if ( event.getType() == Watcher.Event.EventType.NodeDataChanged )
                {
                    int updatedData = toInt( getZooKeeper( true ).getData( path, true, null ) );
                    msgLog.logMessage( "Got event data " + updatedData );
                    if ( path.contains( MASTER_NOTIFY_CHILD ) )
                    {
                        /*
                         * This event is for the masters eyes only so it should only
                         * be the (by zookeeper spoken) master which should make sure
                         * it really is master
                         */

                        if ( updatedData == machineId && !electionHappening )
                        {
                            try
                            {
                                electionHappening = true;
                                Listeners.notifyListeners( zooListeners, new Listeners.Notification<ZooListener>()
                                {
                                    @Override
                                    public void notify( ZooListener listener )
                                    {
                                        listener.masterNotify();
                                    }
                                } );
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
                                Listeners.notifyListeners( zooListeners, new Listeners.Notification<ZooListener>()
                                {
                                    @Override
                                    public void notify( ZooListener listener )
                                    {
                                        listener.masterRebound();
                                    }
                                } );
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
            catch ( Throwable e )
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

    private interface TxIdUpdater
    {
        public void updatedTxId( long txId );

        public void init();
    }

    private abstract class AbstractTxIdUpdater implements TxIdUpdater
    {
        @Override
        public void updatedTxId( long txId )
        {
            // default no op
        }
    }

    private class NoUpdateTxIdUpdater extends AbstractTxIdUpdater
    {
        @Override
        public void init()
        {
            writeData( -2, -2 );
            msgLog.logMessage( "Stopping flushing of txids to zk, " +
                    "while at txid " + committedTx );
        }
    }

    private class CompatibilitySlaveOnlyTxIdUpdater extends AbstractTxIdUpdater
    {
        @Override
        public void init()
        {
            writeData( -1, -1 );
            msgLog.logMessage(
                    "Set to defaults (-1 for txid, -1 for master) since we are running in compatibility mode, " +
                            "while at txid "
                            + committedTx );
        }
    }


    protected ZooKeeperMachine getMasterBasedOn(
            Collection<ZooKeeperMachine> machines )
    {
        ZooKeeperMachine master = null;
        int lowestSeq = Integer.MAX_VALUE;
        long highestTxId = -1;
        for ( ZooKeeperMachine info : machines )
        {
            if ( info.getLastCommittedTxId() != -1 && info.getLastCommittedTxId() >= highestTxId )
            {
                if ( info.getLastCommittedTxId() > highestTxId
                        || info.wasCommittingMaster()
                        || (!master.wasCommittingMaster() && info.getSequenceId() < lowestSeq) )
                {
                    master = info;
                    lowestSeq = info.getSequenceId();
                    highestTxId = info.getLastCommittedTxId();
                }
            }
        }
        log( "getMaster " + (master != null ? master.getMachineId() : "none") +
                " based on " + machines );
        if ( master != null )
        {
            try
            {
                getZooKeeper( false ).getData(
                        getRoot() + "/" + master.getZooKeeperPath(), true, null );
            }
            catch ( KeeperException e )
            {
                throw new ZooKeeperException(
                        "Unable to get master data while setting watch", e );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new ZooKeeperException(
                        "Interrupted while setting watch on master.", e );
            }
            return master;
        }
        else
        {
            return ZooKeeperMachine.NO_MACHINE;
        }
    }

    protected Map<Integer, ZooKeeperMachine> getAllMachines( boolean wait )
    {
        return getAllMachines( wait, WaitMode.SESSION );
    }

    protected Map<Integer, ZooKeeperMachine> getAllMachines( boolean wait, WaitMode mode )
    {
        Map<Integer, ZooKeeperMachine> result = null;
        while ( result == null )
        {
            result = getAllMachinesInner( wait, mode );
        }
        return result;
    }

    protected Map<Integer, ZooKeeperMachine> getAllMachinesInner( boolean wait, WaitMode mode )
    {
        if ( wait )
        {
            waitForSyncConnected( mode );
        }
        try
        {
            /*
             * Optimization - we may have just connected (a fact known by no sequence number
             * available). If we had just gone down we might be trying to read our own
             * entry but that will never be updated. So instead of waiting for the node
             * to expire, just skip it.
             */
            int mySequenceNumber = -1;
            try
            {
                mySequenceNumber = Integer.parseInt( getSequenceNr() );
            }
            catch ( NumberFormatException e )
            {
                // ok, means we are not initialized yet
            }

            writeFlush( getMyMachineId() );

            long endTime = System.currentTimeMillis() + sessionTimeout;
            OUTER:
            do
            {
                Thread.sleep( 100 );
                Map<Integer, ZooKeeperMachine> result = new HashMap<Integer, ZooKeeperMachine>();

                String root = getRoot();
                List<String> children = getZooKeeper( true ).getChildren( root, false );
                for ( String child : children )
                {
                    Pair<Integer, Integer> parsedChild = parseChild( child );
                    if ( parsedChild == null )
                    {   // This was some kind of other ZK node, just ignore
                        continue;
                    }

                    try
                    {
                        int id = parsedChild.first();
                        int seq = parsedChild.other();
                        Pair<Long, Integer> instanceData = readDataRepresentingInstance( root + "/" + child );
                        long lastCommittedTxId = instanceData.first();
                        int masterId = instanceData.other();
                        if ( id == getMyMachineId() && mySequenceNumber == -1 )
                        {   // I'm not initialized yet
                            continue;
                        }
                        if ( lastCommittedTxId == -2 )
                        {   // This instances hasn't written its txId yet. Go out to
                            // the outer loop and retry it again.
                            continue OUTER;
                        }
                        if ( !result.containsKey( id ) || seq > result.get( id ).getSequenceId() )
                        {   // This instance has written its data so I'll grab it.
                            Machine haServer = getHaServer( id, wait );
                            ZooKeeperMachine toAdd = new ZooKeeperMachine( id, seq, lastCommittedTxId, masterId,
                                    haServer.getServerAsString(), haServer.getBackupPort(),
                                    HA_SERVERS_CHILD + "/" + id );
                            result.put( id, toAdd );
                        }
                    }
                    catch ( KeeperException inner )
                    {
                        if ( inner.code() != KeeperException.Code.NONODE )
                        {
                            throw new ZooKeeperException( "Unable to get master.", inner );
                        }
                    }
                }
                return result;
            }
            while ( System.currentTimeMillis() < endTime );
            return null;
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
        finally
        {
            writeFlush( STOP_FLUSHING );
        }
    }

    protected Machine getHaServer( int machineId, boolean wait )
    {
        if ( machineId == this.machineId )
        {
            return asMachine;
        }
        Machine result = haServersCache.get( machineId );
        if ( result == null )
        {
            result = readHaServer( machineId, wait );
            haServersCache.put( machineId, result );
        }
        return result;
    }

    public String getClusterServer()
    {
        return clusterServer;
    }

    public String getHaServer()
    {
        return haServer;
    }

    protected void refreshHaServers() throws KeeperException
    {
        try
        {
            Set<Integer> visitedChildren = new HashSet<Integer>();
            for ( String child : getZooKeeper( true ).getChildren( getRoot() + "/" + HA_SERVERS_CHILD, false ) )
            {
                int id;
                try
                {
                    // We put other nodes under this root, for example "<instance-id>-jmx"
                    // and maybe others. So only include children named with numbers.
                    id = idFromPath( child );
                }
                catch ( NumberFormatException e )
                {
                    continue;
                }
                haServersCache.put( id, readHaServer( id, false ) );
                visitedChildren.add( id );
            }
            haServersCache.keySet().retainAll( visitedChildren );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new ZooKeeperException( "Interrupted", e );
        }
    }

    protected Iterable<Machine> getHaServers()
    {
        return haServersCache.values();
    }

    protected int getNumberOfServers()
    {
        return haServersCache.size();
    }

    protected Machine readHaServer( int machineId, boolean wait )
    {
        if ( wait )
        {
            waitForSyncConnected();
        }
        String rootPath = getRoot();
        String haServerPath = rootPath + "/" + HA_SERVERS_CHILD + "/" + machineId;
        try
        {
            byte[] serverData = getZooKeeper( true ).getData( haServerPath, false, null );
            ByteBuffer buffer = ByteBuffer.wrap( serverData );
            int backupPort = buffer.getInt();
            byte length = buffer.get();
            char[] chars = new char[length];
            buffer.asCharBuffer().get( chars );
            String result = String.valueOf( chars );
            log( "Read HA server:" + result + " (for machineID " + machineId +
                    ") from zoo keeper" );
            return new Machine( machineId, 0, 0, 0, result, backupPort );
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Couldn't find the HA server: " + rootPath, e );
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( "Interrupted", e );
        }
    }

    private void log( String string )
    {
        if ( msgLog != null )
        {
            msgLog.logMessage( string );
        }
    }

    public final void waitForSyncConnected()
    {
        waitForSyncConnected( WaitMode.SESSION );
    }

    enum WaitMode
    {
        STARTUP
                {
                    @Override
                    public WaitStrategy getStrategy( ZooClient zooClient )
                    {
                        return new StartupWaitStrategy( zooClient.msgLog );
                    }
                },
        SESSION
                {
                    @Override
                    public WaitStrategy getStrategy( ZooClient zooClient )
                    {
                        return new SessionWaitStrategy( zooClient.getSessionTimeout() );
                    }
                };

        public abstract WaitStrategy getStrategy( ZooClient zooClient );
    }

    interface WaitStrategy
    {
        abstract boolean waitMore( long waitedSoFar );
    }

    private static class SessionWaitStrategy implements WaitStrategy
    {
        private final long sessionTimeout;

        SessionWaitStrategy( long sessionTimeout )
        {
            this.sessionTimeout = sessionTimeout;
        }

        @Override
        public boolean waitMore( long waitedSoFar )
        {
            return waitedSoFar < sessionTimeout;
        }
    }

    /*
    * Returns int because the ZooKeeper constructor expects an integer,
    * but we are sane and manipulate time as longs.
    */
    protected int getSessionTimeout()
    {
        return (int) sessionTimeout;
    }

    private static class StartupWaitStrategy implements WaitStrategy
    {
        static final long SECONDS_TO_WAIT_BETWEEN_NOTIFICATIONS = 30;

        private long lastNotification = 0;
        private final StringLogger msgLog;

        public StartupWaitStrategy( StringLogger msgLog )
        {
            this.msgLog = msgLog;
        }

        @Override
        public boolean waitMore( long waitedSoFar )
        {
            long currentNotification = waitedSoFar / (SECONDS_TO_WAIT_BETWEEN_NOTIFICATIONS * 1000);
            if ( currentNotification > lastNotification )
            {
                lastNotification = currentNotification;
                msgLog.logMessage( "Have been waiting for " +
                        SECONDS_TO_WAIT_BETWEEN_NOTIFICATIONS
                                * currentNotification + " seconds for the ZooKeeper cluster to respond." );
            }
            return true;
        }
    }

    private String getServersAsString()
    {
        if ( servers.size() == 0 )
        {
            return "";
        }
        StringBuilder string = new StringBuilder( servers.get( 0 ).toString() );
        for ( int i = 1; i < servers.size(); i++ )
        {
            string.append( "," ).append( servers.get( i ).toString() );
        }
        return string.toString();
    }

    protected int idFromPath( String path )
    {
        return Integer.parseInt( path.substring( path.lastIndexOf( '/' ) + 1 ) );
    }

    private void writeFlush( int toWrite )
    {
        final String path = getRoot() + "/" + FLUSH_REQUESTED_CHILD;
        byte[] data = new byte[4];
        ByteBuffer buffer = ByteBuffer.wrap( data );
        buffer.putInt( toWrite );
        boolean created = false;
        try
        {
            if ( getZooKeeper( true ).exists( path, false ) == null )
            {
                try
                {
                    getZooKeeper( true ).create( path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
                    created = true;
                }
                catch ( KeeperException inner )
                {
                    /*
                     *  While we checked, there could be a race and someone else created it
                     *  after we checked but before we create it. That is acceptable.
                     */
                    if ( inner.code() != KeeperException.Code.NODEEXISTS )
                    {
                        throw inner;
                    }
                }
            }
            if ( !created )
            {
                int current = ByteBuffer.wrap( getZooKeeper( true ).getData( path, false, null ) ).getInt();
                if ( current != STOP_FLUSHING && toWrite == STOP_FLUSHING && current != getMyMachineId() )
                {
                    /*
                     *  Someone changed it from what we wrote, we can't just not reset, because that machine
                     *  may be down for the count. Instead wait a bit to finish, then reset, so we don't fall
                     *  into a livelock.
                     */
                    msgLog.logMessage( "Conflicted with " + current
                            + " on getAllMachines() - will reset but waiting a bit" );
                    Thread.sleep( 300 );
                }
                if ( current != toWrite )
                {
                    msgLog.logMessage( "Writing at " + FLUSH_REQUESTED_CHILD + ": " + toWrite );
                    getZooKeeper( true ).setData( path, data, -1 );
                }
            }
            // Set the watch
            getZooKeeper( true ).getData( path, true, null );
        }
        catch ( KeeperException e )
        {
            throw new ZooKeeperException( "Unable to write to " + FLUSH_REQUESTED_CHILD, e );
        }
        catch ( InterruptedException e )
        {
            throw new ZooKeeperException( "Interrupted while trying to write to " + FLUSH_REQUESTED_CHILD, e );
        }
    }

    public static final Machine NO_MACHINE = ZooKeeperMachine.NO_MACHINE;
}
