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

package org.neo4j.kernel;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.transaction.TransactionManager;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.com.ComException;
import org.neo4j.com.IllegalProtocolVersionException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestContext.Tx;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.ClusterEventReceiver;
import org.neo4j.kernel.ha.HaCaches;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClientResolver;
import org.neo4j.kernel.ha.MasterGraphDatabase;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.SlaveDatabaseOperations;
import org.neo4j.kernel.ha.SlaveGraphDatabase;
import org.neo4j.kernel.ha.SlaveServer;
import org.neo4j.kernel.ha.shell.ZooClientFactory;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.NoMasterException;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.GCResistantCacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Loggers;
import org.neo4j.kernel.logging.Logging;

public class HighlyAvailableGraphDatabase
        extends AbstractGraphDatabase implements GraphDatabaseService, GraphDatabaseAPI
{
    private static final int NEW_MASTER_STARTUP_RETRIES = 3;
    public static final String COPY_FROM_MASTER_TEMP = "temp-copy";
    private static final int STORE_COPY_RETRIES = 3;

    private final int localGraphWait;
    protected volatile StoreId storeId;

    private LifeSupport life = new LifeSupport();

    protected Logging logging;
    protected Config configuration;
    private String storeDir;
    private Iterable<IndexProvider> indexProviders;
    private Iterable<KernelExtension> kernelExtensions;
    private Iterable<CacheProvider> cacheProviders;
    private final StringLogger messageLog;
    private volatile InternalAbstractGraphDatabase internalGraphDatabase;
    private NodeProxy.NodeLookup nodeLookup;
    private RelationshipProxy.RelationshipLookups relationshipLookups;

    private final LocalDatabaseOperations slaveOperations;
    private volatile Broker broker;
    private ClusterClient clusterClient;
    private int machineId;
    private volatile MasterServer masterServer;
    private volatile SlaveServer slaveServer;
    private ScheduledExecutorService updatePuller;
    private volatile long updateTime = 0;
    private volatile Throwable causeOfShutdown;
    private long startupTime;
    private BranchedDataPolicy branchedDataPolicy;
    private final SlaveUpdateMode slaveUpdateMode;
    private final Caches caches;
    private final MasterClientResolver masterClientResolver;

    // This lock is used to safeguard access to internal database
    // Users will acquire readlock, and upon master/slave switch
    // a write lock will be acquired
//    private ReadWriteLock databaseLock;

    /*
     *  True iff it is ok to pull updates. Used to control the
     *  update puller during master switches, to reduce could not connect
     *  log statements. More elegant that stopping and starting the executor.
     */
    private volatile boolean pullUpdates;

    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();
    protected final FileSystemAbstraction fileSystemAbstraction;

    /**
     * Default IndexProviders and KernelExtensions by calling Service.load
     *
     * @param storeDir
     * @param config
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config)
    {
        this(storeDir, config, Service.load( IndexProvider.class ), Service.load( KernelExtension.class ),
                Service.load( CacheProvider.class ) );
    }

    /**
     * Create a new instance of HighlyAvailableGraphDatabase
     */
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config, Iterable<IndexProvider> indexProviders,
            Iterable<KernelExtension> kernelExtensions, Iterable<CacheProvider> cacheProviders )
    {
        this.storeDir = storeDir;
        this.indexProviders = indexProviders;
        this.kernelExtensions = kernelExtensions;
        this.cacheProviders = cacheProviders;

        config.put( GraphDatabaseSettings.keep_logical_logs.name(), GraphDatabaseSetting.TRUE);
        config.put( InternalAbstractGraphDatabase.Configuration.store_dir.name(), storeDir );
        if ( !config.containsKey( GraphDatabaseSettings.cache_type.name() ) )
            config.put( GraphDatabaseSettings.cache_type.name(), GCResistantCacheProvider.NAME );

        // Setup configuration
        configuration = new Config( config, GraphDatabaseSettings.class, HaSettings.class, OnlineBackupSettings.class );

        // Create logger
        this.logging = createLogging();

        messageLog = logging.getLogger( Loggers.NEO4J );

        configuration.setLogger(messageLog);

        fileSystemAbstraction = new DefaultFileSystemAbstraction();

        caches = new HaCaches( messageLog );

        /*
         * TODO
         * lame, i know, but better than before.
         */
        slaveOperations = new LocalDatabaseOperations();

        // databaseLock = new ReentrantReadWriteLock( );

        this.nodeLookup = new HANodeLookup();

        this.relationshipLookups = new HARelationshipLookups();

        this.startupTime = System.currentTimeMillis();
        kernelEventHandlers.add( new TxManagerCheckKernelEventHandler() );

        this.slaveUpdateMode = configuration.getEnum( SlaveUpdateMode.class, HaSettings.slave_coordinator_update_mode );
        this.machineId = configuration.getInteger( HaSettings.server_id );
        this.branchedDataPolicy = configuration.getEnum( BranchedDataPolicy.class, HaSettings.branched_data_policy );
        this.localGraphWait = configuration.getInteger( HaSettings.read_timeout );

        this.masterClientResolver = new MasterClientResolver(
                messageLog,
                configuration.getInteger( HaSettings.read_timeout ),
                configuration.isSet( HaSettings.lock_read_timeout ) ? configuration.getInteger( HaSettings.lock_read_timeout )
                        : configuration.getInteger( HaSettings.read_timeout ),
                configuration.getInteger( HaSettings.max_concurrent_channels_per_slave ) );
        masterClientResolver.getDefault();
        // TODO The dependency from BrokerFactory to 'this' is completely
        // broken. Needs rethinking
        this.broker = createBroker();
        this.pullUpdates = false;
        this.clusterClient = createClusterClient();

        migrateBranchedDataDirectoriesToRootDirectory();

        start();
    }

    private void migrateBranchedDataDirectoriesToRootDirectory()
    {
        File branchedDir = BranchedDataPolicy.getBranchedDataRootDirectory( storeDir );
        branchedDir.mkdirs();
        for ( File oldBranchedDir : new File( storeDir ).listFiles() )
        {
            if ( !oldBranchedDir.isDirectory() || !oldBranchedDir.getName().startsWith( "branched-" ) )
                continue;

            long timestamp = 0;
            try
            {
                timestamp = Long.parseLong( oldBranchedDir.getName().substring( oldBranchedDir.getName().indexOf( '-' ) + 1 ) );
            }
            catch ( NumberFormatException e )
            {   // OK, it wasn't a branched directory after all.
                continue;
            }

            File targetDir = BranchedDataPolicy.getBranchedDataDirectory( storeDir, timestamp );
            try
            {
                FileUtils.moveFile( oldBranchedDir, targetDir );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Couldn't move branched directories to " + branchedDir, e );
            }
        }
    }

    private Logging createLogging()
    {
        try
        {
            getClass().getClassLoader().loadClass("ch.qos.logback.classic.LoggerContext");
            return life.add( new LogbackService( configuration ));
        }
        catch( ClassNotFoundException e )
        {
            return life.add( new ClassicLoggingService(configuration));
        }
    }

    // GraphDatabaseService implementation
    // TODO This pattern is broken. Should lock database for duration of call, not just on access of db
    @Override
    public Node createNode()
    {
        return localGraph().createNode();
    }

    @Override
    public Node getNodeById( long id )
    {
        return localGraph().getNodeById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return localGraph().getReferenceNode();
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return localGraph().getAllNodes();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return localGraph().getRelationshipTypes();
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return localGraph().getRelationshipById( id );
    }

    @Override
    public IndexManager index()
    {
        return localGraph().index();
    }

    @Override
    public Transaction beginTx()
    {
        return localGraph().beginTx();
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown( new IllegalStateException( "shutdown called" ), true );
    }

    // GraphDatabaseSPI implementation

    @Override
    public NodeManager getNodeManager()
    {
        return localGraph().getNodeManager();
    }

    @Override
    public LockReleaser getLockReleaser()
    {
        return localGraph().getLockReleaser();
    }

    @Override
    public LockManager getLockManager()
    {
        return localGraph().getLockManager();
    }

    @Override
    public XaDataSourceManager getXaDataSourceManager()
    {
        return localGraph().getXaDataSourceManager();
    }

    @Override
    public TransactionManager getTxManager()
    {
        return localGraph().getTxManager();
    }

    @Override
    public DiagnosticsManager getDiagnosticsManager()
    {
        return localGraph().getDiagnosticsManager();
    }

    @Override
    public StringLogger getMessageLog()
    {
        return messageLog;
    }

    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return localGraph().getRelationshipTypeHolder();
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return localGraph().getIdGeneratorFactory();
    }

    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return localGraph().getTxIdGenerator();
    }

    @Override
    public KernelData getKernelData()
    {
        return localGraph().getKernelData();
    }

    public <T> T getSingleManagementBean( Class<T> type )
    {
        return localGraph().getSingleManagementBean( type );
    }

    // Internal
    private void getFreshDatabaseFromMaster( boolean branched )
    {
        /*
         * Don't be connected to ZK while copying the store from the master. This way we don't
         * get interrupted from master elections. We'll get only one event, the ComException
         * and we'll retry. Sandboxing this makes sure that if we are the new master nothing
         * was lost.
         */
        broker.shutdown();
        try
        {
            /*
             * Use the cluster client here instead of the broker provided master client.
             * The problem is that clients from the broker are shutdown when zk hiccups
             * so the channel is closed and the copy operation fails. Clients provided from
             * the clusterClient do not suffer from that - after getting hold of such an
             * object, even if the zk cluster goes down the operation will succeed, dependent
             * only on the source machine being alive. If, in the meantime, the master changes
             * then the verification after the new master election will call us again.
             */
            Pair<Master, Machine> master = createClusterClient().getMasterClient();

            // Assume it's shut down at this point
            internalShutdown( false );

            if ( branched )
            {
                makeWayForNewDb();
            }
            Exception exception = null;
            for ( int i = 0; i < STORE_COPY_RETRIES; i++ )
            {
                try
                {
                    /*
                     * Either we branched so the previous store is not there
                     * or we did not detect a neostore file so the db is
                     * incomplete. Either way, it is safe to delete everything.
                     */
                    BranchedDataPolicy.keep_none.handle( this );
                    copyStoreFromMaster( master );
                    moveCopiedStoreIntoWorkingDir();
                    return;
                }
                // TODO Maybe catch IOException and treat it more seriously?
                catch ( Exception e )
                {
                    getMessageLog().logMessage(
                            "Problems copying store from master", e );
                    sleepWithoutInterruption( 1000, "" );
                    exception = e;
                    // Stuff in the cluster might have changed - reread.
                    master = clusterClient.getMasterClient();
                }
            }
            throw new RuntimeException(
                    "Gave up trying to copy store from master", exception );
        }
        finally
        {
            // No matter what, start the broker again
            broker.start();
        }
    }

    private File getTempDir()
    {
        return new File( getStoreDir(), COPY_FROM_MASTER_TEMP );
    }

    /**
     * Moves all files from the temp directory to the current working directory.
     * Assumes the target files do not exist and skips over the messages.log
     * file the temp db creates.
     * @throws IOException if move wasn't successful.
     */
    private void moveCopiedStoreIntoWorkingDir() throws IOException
    {
        File storeDir = new File( getStoreDir() );
        for ( File candidate : getTempDir().listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File file )
            {
                return !file.getName().equals( StringLogger.DEFAULT_NAME );
            }
        } ) )
        {
            FileUtils.moveFileToDirectory( candidate, storeDir );
        }
    }

    /**
     * Clears out the temp directory from all files contained and returns the
     * path to it.
     *
     * @return The path to the directory used as a sandbox for store copies.
     * @throws IOException if any IO error occurs
     */
    private File getClearedTempDir() throws IOException
    {
        File temp = getTempDir();
        if ( !temp.mkdir() )
        {
            FileUtils.deleteRecursively( temp );
            temp.mkdir();
        }
        return temp;

    }

    void makeWayForNewDb()
    {
        this.messageLog.logMessage( "Cleaning database " + storeDir + " (" + branchedDataPolicy.name() +
                                         ") to make way for new db from master" );
        branchedDataPolicy.handle( this );
    }

    protected void start()
    {
        life.start();

        getMessageLog().logMessage( "Starting up highly available graph database '" + getStoreDir() + "'" );

        if ( !new File( storeDir, NeoStore.DEFAULT_NAME ).exists() )
        {   // Try for
            long endTime = System.currentTimeMillis()+60000;
            Exception exception = null;
            while ( System.currentTimeMillis() < endTime )
            {
                // Check if the cluster is up
                Pair<Master, Machine> master = broker.bootstrap();// getMasterReally(
                                                                  // true );

                if ( master != null && !master.other().equals( Machine.NO_MACHINE ) &&
                        master.other().getMachineId() != machineId )
                {   // Join the existing cluster
                    try
                    {
                        getFreshDatabaseFromMaster( false /*branched*/);
                        messageLog.logMessage( "copied store from master" );
                        exception = null;
                        break;
                    }
                    catch ( Exception e )
                    {
                        exception = e;
                        master = broker.getMasterReally( true );
                        messageLog.logMessage( "Problems copying store from master", e );
                    }
                }
                else
                {   // I seem to be the master, the broker have created the cluster for me
                    // I'm just going to start up now
//                    storeId = broker.getClusterStoreId();
                    break;
                }
                // I am not master, and could not connect to the master:
                // wait for other machine(s) to join.
                sleepWithoutInterruption( 300, "Startup interrupted" );
            }

            if ( exception != null )
            {
                throw new RuntimeException( "Tried to join the cluster, but was unable to", exception );
            }
        }
        storeId = broker.getClusterStoreId( true );
        newMaster( new InformativeStackTrace( "Starting up [" + machineId + "] for the first time" ) );
        localGraph();
    }

    private void checkAndRecoverCorruptLogs( InternalAbstractGraphDatabase localDb,
            boolean copiedStore )
    {
        getMessageLog().logMessage( "Checking for log consistency" );
        /*
         * We are going over all data sources and try to retrieve the latest transaction. If that fails then
         * the logs might be missing or corrupt. Try to recover by asking the master for the transaction and
         * either patch the current log file or recreate the missing one.
         */
        XaDataSource dataSource = localDb.getXaDataSourceManager().getNeoStoreDataSource();
        getMessageLog().logMessage( "Checking dataSource " + dataSource.getName() );
        boolean corrupted = false;
        long version = -1; // the log version, -1 indicates current log
        long myLastCommittedTx = dataSource.getLastCommittedTxId();
        if ( myLastCommittedTx == 1 )
        {
            // The case of a brand new store, nothing to do
            return;
        }
        try
        {
            int masterId = dataSource.getMasterForCommittedTx( myLastCommittedTx ).first();
            if ( masterId == -1 )
            {
                corrupted = true;
            }
        }
        catch ( NoSuchLogVersionException e )
        {
            getMessageLog().logMessage(
                    "Missing log version " + e.getVersion()
                    + " for transaction " + myLastCommittedTx
                    + " and datasource " + dataSource.getName() );
            corrupted = true;
            version = e.getVersion();
        }
        catch ( IOException e )
        {
            getMessageLog().logMessage(
                    "IO exceptions while trying to retrieve the master for the latest txid (= "
                            + myLastCommittedTx + " )", e );
        }
        catch ( RuntimeException e )
        {
            getMessageLog().logMessage(
                    "Runtime exception while getting master id for"
                            + " for transaction " + myLastCommittedTx
                            + " and datasource " + dataSource.getName(), e );
            corrupted = true;
            /*
             * We have no available way to know where it should be - just
             * overwrite the last one
             */
            version = dataSource.getCurrentLogVersion() - 1;
        }
        if ( corrupted )
        {
            if ( version != -1 )
            {
                getMessageLog().logMessage(
                        "Logical log file for transaction "
                                + myLastCommittedTx + " not found." );
            }
            else
            {
                getMessageLog().logMessage(
                        "Tried to extract transaction "
                                + myLastCommittedTx
                                + " but it was not present in the log. Trying to retrieve it from master." );
            }
            if ( copiedStore )
            {
                /*
                 * We copied the store, so there may be pending stuff to write to disk. No point in
                 * checking for log existence/sanity, since even if an error is detected we can
                 * attribute it to the copy operation being in progress. Just warn then.
                 */
                getMessageLog().logMessage(
                        "A store copy might be in progress. Will not act on the apparent corruption" );
            }
            else
            {
                try
                {
                    copyLogFromMaster( broker.getMaster(),
                            Config.DEFAULT_DATA_SOURCE_NAME, version,
                            myLastCommittedTx, myLastCommittedTx );
                    // Rechecking, might cost something extra but worth it
                    dataSource.getMasterForCommittedTx( myLastCommittedTx );
                    getMessageLog().logMessage(
                            "Log copy finished without problems" );
                }
                catch ( Exception e )
                {
                    getMessageLog().logMessage(
                            "Failed to retrieve log version "
                                    + version + " from master.", e );
                }
            }
        }
    }

    /**
    * Tries to get a set of transactions for a specific data source from the
    * master and possibly write it out as a versioned log file. Useful for
    * recovering your damaged or missing log files.
    *
    * @param master The master to retrieve transactions from
    * @param datasource The datasource for which the txs to retrieve
    * @param logVersion The version of the log to rebuild, with -1 indicating
    * apply to current one
    * @param startTxId The first tx to retrieve
    * @param endTxId The last tx to retrieve
    * @throws Exception
    */
    private void copyLogFromMaster( Pair<Master, Machine> master,
            String datasource, long logVersion, long startTxId, long endTxId )
                    throws Exception
    {
        Response<Void> response = master.first().copyTransactions( emptyContext(), datasource,
                startTxId, endTxId );
        if ( logVersion == -1 )
        {
            // No log version, just apply to the latest one
            slaveOperations.receive( response );
            return;
        }
        XaDataSource ds = localGraph().getXaDataSourceManager().getXaDataSource(
                datasource );
        FileChannel newLog = localGraph().fileSystem.create( ds.getFileName( logVersion ) );
        newLog.truncate( 0 );
        ByteBuffer scratch = ByteBuffer.allocate( 64 );
        LogIoUtils.writeLogHeader( scratch, logVersion, startTxId );
        // scratch buffer is flipped by writeLogHeader
        newLog.write( scratch );
        ReadableByteChannel received = response.transactions().next().third().extract();
        scratch.flip();
        while ( received.read( scratch ) > 0 )
        {
            scratch.flip();
            newLog.write( scratch );
            scratch.flip();
        }
        newLog.force( false );
        newLog.close();

    }

    private void sleepWithoutInterruption( long time, String errorMessage )
    {
        try
        {
            Thread.sleep( time );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( errorMessage, e );
        }
    }

    private void copyStoreFromMaster( Pair<Master, Machine> master )
            throws Exception
    {
        getMessageLog().logMessage( "Copying store from master" );
        String temp = getClearedTempDir().getAbsolutePath();
        Response<Void> response = master.first().copyStore( emptyContext(), new ToFileStoreWriter( temp ) );
        long highestLogVersion = highestLogVersion( temp );
        if( highestLogVersion > -1 )
        {
            NeoStore.setVersion( temp, highestLogVersion + 1 );
        }
        GraphDatabaseAPI copiedDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( temp ).setConfig(
                GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).setConfig(
                GraphDatabaseSettings.allow_store_upgrade,
                configuration.get( GraphDatabaseSettings.allow_store_upgrade ).toString() ).

            newGraphDatabase();

        try
        {
            ServerUtil.applyReceivedTransactions( response, copiedDb, ServerUtil.txHandlerForFullCopy() );
        }
        finally
        {
            copiedDb.shutdown();
            response.close();
        }
        getMessageLog().logMessage( "Done copying store from master" );
    }

    private RequestContext emptyContext()
    {
        return new RequestContext( 0, machineId, 0, new Tx[0], 0, 0 );
    }

    private long highestLogVersion( String targetStoreDir )
    {
        return XaLogicalLog.getHighestHistoryLogVersion( new File( targetStoreDir ), LOGICAL_LOG_DEFAULT_NAME );
    }

    /**
     * Access to the internal database reference. Uses a read-lock to ensure that we are not currently restarting it
     *
     * @return
     */
    private InternalAbstractGraphDatabase localGraph()
    {
        InternalAbstractGraphDatabase result = internalGraphDatabase;
        if( result != null )
        {
            return result;
        }

        long endTime = System.currentTimeMillis()+SECONDS.toMillis( localGraphWait );
        while ( result == null && System.currentTimeMillis() < endTime )
        {
            sleepWithoutInterruption( 1, "Failed waiting for local graph to be available" );
            result = internalGraphDatabase;
        }
        if( result != null )
        {
            return result;
        }

        if ( causeOfShutdown != null )
        {
            throw new RuntimeException( "Graph database not started", causeOfShutdown );
        }
        else
        {
            throw new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                    "maybe not started yet or in the middle of master/slave swap?" );
        }
    }

    public Broker getBroker()
    {
        return this.broker;
    }

    public void pullUpdates()
    {
        try
        {
            if ( masterServer == null )
            {
                if ( broker.getMaster() == null
                     && broker instanceof ZooKeeperBroker )
                {
                    /*
                     * Log a message - the ZooKeeperBroker should not return
                     * null master
                     */
                    messageLog.logMessage(
                            "ZooKeeper broker returned null master" );
                    newMaster( new NullPointerException(
                            "master returned from broker" ) );
                }
                else if ( broker.getMaster().first() == null )
                {
                    newMaster( new NullPointerException(
                            "master returned from broker" ) );
                }

                RequestContext slaveContext = null;
                // If this method is called from the outside then we need to tell the caller
                // that this update wasn't performed due to either a shutdown or an internal restart,
                // so throw NoMasterException
                if ( !pullUpdates )
                    throw new NoMasterException();
                synchronized ( this )
                {
                    // If we got the monitor and pullUpdates is false this means that we've
                    // just shut down the database. Don't do pull updates then and be done.
                    if ( !pullUpdates )
                        return;
                    slaveContext = slaveOperations.getSlaveContext( -1 );
                }

                // The above synchronization only guards for getting the SlaveContext,
                // but an internal(shutdown) can still happen in the middle of receive.
                // This is a general problem which should be taken care of in a general
                // way, not here.
                slaveOperations.receive( broker.getMaster().first().pullUpdates( slaveContext ) );
            }
        }
        catch ( ZooKeeperException e )
        {
            newMaster( e );
            throw e;
        }
        catch ( NoMasterException e )
        {
            newMaster( e );
            throw e;
        }
        catch ( ComException e )
        {
            /*
             * A ComException means connection to the master could not be established.
             * It is generally wrong to take this a sign to perform master election. The
             * failure might be transient, the broker data might not be updated yet (a
             * very real possibility for ZK specific installations) etc. So just throw the
             * exception and hope that if the failure is real newMaster() will be called
             * eventually
             */
            // newMaster( e );
            throw e;
        }
    }

    private void updateTime()
    {
        this.updateTime = System.currentTimeMillis();
    }

    long lastUpdateTime()
    {
        return this.updateTime;
    }

    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return localGraph().getManagementBeans( type );
    }

    @Override
    public boolean transactionRunning()
    {
        return localGraph().transactionRunning();
    }

    public final <T> T getManagementBean( Class<T> type )
    {
        return localGraph().getManagementBean( type );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + storeDir + ", " + HaSettings.server_id.name() + ":" + machineId + "]";
    }

    protected synchronized void reevaluateMyself()
    {
        Pair<Master, Machine> master = broker.getMasterReally( true );
        boolean iAmCurrentlyMaster = masterServer != null;
        getMessageLog().logMessage( "ReevaluateMyself: machineId=" + machineId + " with master[" + master +
                "] (I am master=" + iAmCurrentlyMaster + ", " + internalGraphDatabase + ")" );
        pullUpdates = false;
        InternalAbstractGraphDatabase newDb = null;
        try
        {
            if ( master.other().getMachineId() == machineId )
            { // I am the new master
                if ( this.internalGraphDatabase == null || !iAmCurrentlyMaster )
                { // I am currently a slave, so restart as master
                    internalShutdown( true );
                    newDb = startAsMaster();
                }
                // fire rebound event
                broker.rebindMaster();
            }
            else
            { // Someone else is master
                broker.notifyMasterChange( master.other() );
                if ( this.internalGraphDatabase == null || iAmCurrentlyMaster )
                { // I am currently master, so restart as slave.
                    // This will result in clearing of free ids from .id files, see SlaveIdGenerator.
                    internalShutdown( true );
                    newDb = startAsSlave();
                }
                else
                { // I am already a slave, so just forget the ids I got from the previous master
                    ((SlaveGraphDatabase)internalGraphDatabase).forgetIdAllocationsFromMaster();
                }
            }
            if ( masterServer == null )
            {
                // The above being true means we are a slave
                instantiateAutoUpdatePullerIfConfigSaysSo();
                checkAndRecoverCorruptLogs( newDb != null ? newDb : internalGraphDatabase,
                        false );
                ensureDataConsistencyWithMaster( newDb != null ? newDb
                        : internalGraphDatabase, master );
                getMessageLog().logMessage( "Data consistent with master" );
            }
            if ( newDb != null )
            {
                doAfterLocalGraphStarted( newDb );

                // Assign the db last so that no references leak
                this.internalGraphDatabase = newDb;
                // Now ok to pull updates
            }
            pullUpdates = true;
        }
        catch ( Throwable t )
        {
            safelyShutdownDb( newDb );
            throw launderedException( t );
        }
    }

    private void safelyShutdownDb( InternalAbstractGraphDatabase newDb )
    {
        try
        {
            if( newDb != null )
            {
                newDb.shutdown();
            }
        }
        catch ( Exception e )
        {
            messageLog.logMessage( "Couldn't shut down newly started db", e );
        }
    }

    private void doAfterLocalGraphStarted( InternalAbstractGraphDatabase newDb )
    {
        broker.setConnectionInformation( newDb.getKernelData() );
        for ( TransactionEventHandler<?> handler : transactionEventHandlers )
        {
            newDb.registerTransactionEventHandler( handler );
        }
        for ( KernelEventHandler handler : kernelEventHandlers )
        {
            newDb.registerKernelEventHandler( handler );
        }
    }

    private void logHaInfo( String started )
    {
        messageLog.logMessage( started, true );
        messageLog.logMessage( "--- HIGH AVAILABILITY CONFIGURATION START ---" );
        broker.logStatus( messageLog );
        messageLog.logMessage( "--- HIGH AVAILABILITY CONFIGURATION END ---", true );
    }

    private InternalAbstractGraphDatabase startAsSlave()
    {
        messageLog.logMessage( "Starting[" + machineId + "] as slave", true );
        SlaveGraphDatabase slaveGraphDatabase = new SlaveGraphDatabase( storeDir, configuration.getParams(), storeId, this, broker, logging,
                slaveOperations, slaveUpdateMode.createUpdater( broker ), nodeLookup,
                relationshipLookups, fileSystemAbstraction, indexProviders, kernelExtensions, cacheProviders, caches );
        this.slaveServer = (SlaveServer) broker.instantiateSlaveServer( this, slaveOperations );
        logHaInfo( "Started as slave" );
        this.startupTime = System.currentTimeMillis();
        return slaveGraphDatabase;
    }

    private InternalAbstractGraphDatabase startAsMaster()
    {
        messageLog.logMessage( "Starting[" + machineId + "] as master", true );

        MasterGraphDatabase master = new MasterGraphDatabase( storeDir, configuration.getParams(), storeId, this,
                broker, logging, nodeLookup, relationshipLookups, indexProviders, kernelExtensions, cacheProviders, caches);
        this.masterServer = (MasterServer) broker.instantiateMasterServer( master );
        logHaInfo( "Started as master" );
        this.startupTime = System.currentTimeMillis();
        return master;
    }

    // TODO This should be moved to SlaveGraphDatabase
    private void ensureDataConsistencyWithMaster( InternalAbstractGraphDatabase newDb, Pair<Master, Machine> master )
    {
        if ( master.other().getMachineId() == machineId )
        {
            getMessageLog().logMessage( "I am master so cannot consistency check data with master" );
            return;
        }
        else if ( master.first() == null )
        {
            // Temporarily disconnected from ZK
            RuntimeException cause = new RuntimeException( "Unable to get master from ZK" );
            shutdown( cause, false );
            throw cause;
        }

        XaDataSource nioneoDataSource = newDb.getXaDataSourceManager().getNeoStoreDataSource();
        long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
        Pair<Integer,Long> myMaster;
        try
        {
            myMaster = nioneoDataSource.getMasterForCommittedTx( myLastCommittedTx );
        }
        catch ( NoSuchLogVersionException e )
        {
            getMessageLog().logMessage(
                    "Logical log file for txId "
                            + myLastCommittedTx
                               + " missing [version="
                               + e.getVersion()
                               + "]. If this is startup then it will be recovered later, otherwise it might be a problem." );
            return;
        }
        catch ( IOException e )
        {
            getMessageLog().logMessage( "Failed to get master ID for txId " + myLastCommittedTx + ".", e );
            return;
        }
        catch ( Exception e )
        {
            getMessageLog().logMessage(
                    "Exception while getting master ID for txId "
                            + myLastCommittedTx + ".", e );
            throw new BranchedDataException( "Maybe not branched data, but it could solve it", e );
        }

        Response<Pair<Integer, Long>> response = null;
        Pair<Integer, Long> mastersMaster;
        try
        {
            response = master.first().getMasterIdForCommittedTx( myLastCommittedTx, newDb.getStoreId() );
            mastersMaster = response.response();
        }
        catch ( RuntimeException e )
        {
            if ( e.getCause() instanceof NoSuchLogVersionException )
            {
                /*
                 * This means the master was unable to find a log entry for the txid we just asked. This
                 * probably means the thing we asked for is too old or too new. Anyway, since it doesn't
                 * have the tx it is better if we just throw our store away and ask for a new copy. Next
                 * time around it shouldn't have to even pass from here.
                 */
                throw new BranchedDataException( "Maybe not branched data, but it could solve it", e.getCause() );
            }
            else
            {
                throw e;
            }
        }
        finally
        {
            if ( response != null )
            {
                response.close();
            }
        }

        if ( myMaster.first() != XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER
            && !myMaster.equals( mastersMaster ) )
        {
            String msg = "Branched data, I (machineId:" + machineId + ") think machineId for txId (" +
                    myLastCommittedTx + ") is " + myMaster + ", but master (machineId:" +
                    master.other().getMachineId() + ") says that it's " + mastersMaster;
            getMessageLog().logMessage( msg, true );
            RuntimeException exception = new BranchedDataException( msg );
            safelyShutdownDb( newDb );
            shutdown( exception, false );
            throw exception;
        }
        getMessageLog().logMessage( "Master id for last committed tx ok with highestTxId=" +
            myLastCommittedTx + " with masterId=" + myMaster, true );
    }

    private void instantiateAutoUpdatePullerIfConfigSaysSo()
    {
        long pullInterval = configuration.getDuration( HaSettings.pull_interval );
        if ( pullInterval > 0 && updatePuller == null )
        {
            updatePuller = new ScheduledThreadPoolExecutor( 1 );
            updatePuller.scheduleWithFixedDelay( new Runnable()
            {
                @Override
                public void run()
                {
                    if ( !pullUpdates )
                    {
                        return;
                    }
                    try
                    {
                        pullUpdates();
                    }
                    catch ( Exception e )
                    {
                        messageLog.logMessage( "Pull updates failed", e  );
                    }
                }
            }, pullInterval, pullInterval, TimeUnit.MILLISECONDS );
        }
    }

    public TransactionBuilder tx()
    {
        return localGraph().tx();
    }

    public synchronized void internalShutdown( boolean rotateLogs )
    {
        messageLog.logMessage( "Internal shutdown of HA db[" + machineId + "] reference=" + this + ", masterServer=" + masterServer, new InformativeStackTrace( "Internal shutdown" ), true );
        pullUpdates = false;
        if ( this.updatePuller != null )
        {
            messageLog.logMessage( "Internal shutdown updatePuller", true );
            try
            {
                /*
                 * Be gentle, interrupting running threads could leave the
                 * file channels in a bad shape.
                 */
                this.updatePuller.shutdown();
                this.updatePuller.awaitTermination( 5, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                messageLog.logMessage(
                        "Got exception while waiting for update puller termination",
                        e, true );
            }
            messageLog.logMessage( "Internal shutdown updatePuller DONE",
                    true );
            this.updatePuller = null;
        }
        if ( this.masterServer != null )
        {
            messageLog.logMessage( "Internal shutdown masterServer", true );
            this.masterServer.shutdown();
            messageLog.logMessage( "Internal shutdown masterServer DONE", true );
            this.masterServer = null;
        }
        if ( this.slaveServer != null )
        {
            this.slaveServer.shutdown();
            this.slaveServer = null;
        }
        if ( this.internalGraphDatabase != null )
        {
            if ( rotateLogs )
            {
                internalGraphDatabase.getXaDataSourceManager().rotateLogicalLogs();
            }
            messageLog.logMessage( "Internal shutdown localGraph", true );
            this.internalGraphDatabase.shutdown();
            messageLog.logMessage( "Internal shutdown localGraph DONE", true );
            this.internalGraphDatabase = null;
        }
        messageLog.flush();
    }

    private synchronized void shutdown( Throwable cause, boolean shutdownBroker )
    {
        causeOfShutdown = cause;
        messageLog.logMessage( "Shutdown[" + machineId + "], " + this, true );
        if ( shutdownBroker && this.broker != null )
        {
            this.broker.shutdown();
        }
        internalShutdown( false );

        life.shutdown();
    }

    protected synchronized void close()
    {
        shutdown( new IllegalStateException( "shutdown called" ), true );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        this.kernelEventHandlers.add( handler );
        return localGraph().registerKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        this.transactionEventHandlers.add( handler );
        return localGraph().registerTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        try
        {
            return localGraph().unregisterKernelEventHandler( handler );
        }
        finally
        {
            kernelEventHandlers.remove( handler );
        }
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        try
        {
            return localGraph().unregisterTransactionEventHandler( handler );
        }
        finally
        {
            transactionEventHandlers.remove( handler );
        }
    }

    private synchronized void newMaster( Exception e )
    {
        /* MP: This is from BranchDetectingTxVerifier which can report branched data via a
         * BranchedDataException embedded inside a ComException (just to pass through the usual
         * code paths w/o any additional code). Feel free to refactor to get rid of this packing */
        if ( e instanceof ComException && e.getCause() instanceof BranchedDataException )
        {
            BranchedDataException bde = (BranchedDataException) e.getCause();
            getMessageLog().logMessage( "Master says I've got branched data: " + bde );
        }

        Throwable cause = null;
        int i = 0;
        boolean unexpectedException = false;
        while ( i++ < NEW_MASTER_STARTUP_RETRIES )
        {
            try
            {
                getMessageLog().logMessage( "newMaster called", e, true );
                reevaluateMyself();
                return;
            }
            catch ( ZooKeeperException zke )
            {
                getMessageLog().logMessage(
                        "ZooKeeper exception in newMaster, retry #" + i, zke );
                e = zke;
                cause = zke;
                sleepWithoutInterruption( 500, "" );
                continue;
            }
            catch ( IllegalProtocolVersionException pe )
            {
                getMessageLog().logMessage(
                        "Got wrong protocol version during newMaster, expected " + pe.getExpected() + " but got "
                                + pe.getReceived(), e );
                broker.restart();
                e = pe;
                cause = pe;
                continue;
            }
            catch ( ComException ce )
            {
                getMessageLog().logMessage(
                        "Communication exception in newMaster, retry #" + i, ce );
                e = ce;
                cause = ce;
                sleepWithoutInterruption( 500, "" );
                continue;
            }
            catch ( BranchedDataException bde )
            {
                getMessageLog().logMessage(
                        "Branched data occurred, during newMaster retry #" + i,
                        bde );
                getFreshDatabaseFromMaster( true /*branched*/);
                e = bde;
                cause = bde;
                continue;
            }
            catch ( Throwable t )
            {
                cause = t;
                unexpectedException = true;
                break;
            }
        }
        if ( cause != null && unexpectedException )
        {
            getMessageLog().logMessage(
                    "Reevaluation ended in unknown exception " + cause
                    + " so shutting down", cause, true );
            shutdown( cause, false );
        }
        throw Exceptions.launderedException( cause );
    }

    public MasterServer getMasterServerIfMaster()
    {
        return masterServer;
    }

    protected int getMachineId()
    {
        return machineId;
    }

    public boolean isMaster()
    {
        return getMasterServerIfMaster() != null;
    }

    protected Broker createBroker()
    {
        return new ZooKeeperBroker( configuration, new ZooClientFactory()
        {
            @Override
            public ZooClient newZooClient()
            {
                return new ZooClient( storeDir, messageLog, configuration, /* as SlaveDatabaseOperations for extracting master for tx */
                slaveOperations, /* as ClusterEventReceiver */slaveOperations, masterClientResolver );
            }
        } );
    }

    protected ClusterClient createClusterClient()
    {
        return defaultClusterClient();
    }

    private ClusterClient defaultClusterClient()
    {
        return new ZooKeeperClusterClient( configuration.get( HaSettings.coordinators ), getMessageLog(),
                configuration.get( HaSettings.cluster_name ),
                configuration.getInteger( HaSettings.zk_session_timeout ), masterClientResolver );
    }

    // TODO This should be removed. Analyze usages
    public String getStoreDir()
    {
        return storeDir;
    }
    
    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return localGraph().txIdGenerator;
    }

    @Override
    public KernelPanicEventGenerator getKernelPanicGenerator()
    {
        return localGraph().getKernelPanicGenerator();
    }

    public static enum BranchedDataPolicy
    {
        keep_all
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                moveAwayDb( db, newBranchedDataDir( db ) );
            }
        },
        keep_last
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                File branchedDataDir = newBranchedDataDir( db );
                moveAwayDb( db, branchedDataDir );
                for ( File file : getBranchedDataRootDirectory( db.getStoreDir() ).listFiles() )
                {
                    if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                    {
                        try
                        {
                            FileUtils.deleteRecursively( file );
                        }
                        catch ( IOException e )
                        {
                            db.messageLog.logMessage( "Couldn't delete old branched data directory " + file, e );
                        }
                    }
                }
            }
        },
        keep_none
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                for ( File file : relevantDbFiles( db ) )
                {
                    try
                    {
                        FileUtils.deleteRecursively( file );
                    }
                    catch ( IOException e )
                    {
                        db.messageLog.logMessage( "Couldn't delete file " + file, e );
                    }
                }
            }
        },
        shutdown
        {
            @Override
            void handle( HighlyAvailableGraphDatabase db )
            {
                db.shutdown();
            }
        };

        // Branched directories will end up in <dbStoreDir>/branched/<timestamp>/
        static String BRANCH_SUBDIRECTORY = "branched";

        abstract void handle( HighlyAvailableGraphDatabase db );

        protected void moveAwayDb( HighlyAvailableGraphDatabase db, File branchedDataDir )
        {
            for ( File file : relevantDbFiles( db ) )
            {
                try
                {
                    FileUtils.moveFileToDirectory( file, branchedDataDir );
                }
                catch ( IOException e )
                {
                    db.messageLog.logMessage( "Couldn't move " + file.getPath() );
                }
            }
        }

        File newBranchedDataDir( HighlyAvailableGraphDatabase db )
        {
            File result = getBranchedDataDirectory( db.getStoreDir(), System.currentTimeMillis() );
            result.mkdirs();
            return result;
        }

        File[] relevantDbFiles( HighlyAvailableGraphDatabase db )
        {
            if (!new File( db.getStoreDir() ).exists())
                return new File[0];

            return new File( db.getStoreDir() ).listFiles( new FileFilter()
            {
                @Override
                public boolean accept( File file )
                {
                    return !file.getName().equals( StringLogger.DEFAULT_NAME ) && !isBranchedDataRootDirectory( file );
                }
            } );
        }

        public static boolean isBranchedDataRootDirectory( File directory )
        {
            return directory.isDirectory() && directory.getName().equals( BRANCH_SUBDIRECTORY );
        }

        public static boolean isBranchedDataDirectory( File directory )
        {
            return directory.isDirectory() && directory.getParentFile().getName().equals( BRANCH_SUBDIRECTORY ) &&
                    isAllDigits( directory.getName() );
        }

        private static boolean isAllDigits( String string )
        {
            for ( char c : string.toCharArray() )
                if ( !Character.isDigit( c ) )
                    return false;
            return true;
        }

        public static File getBranchedDataRootDirectory( String dbStoreDir )
        {
            return new File( dbStoreDir, BRANCH_SUBDIRECTORY );
        }

        public static File getBranchedDataDirectory( String dbStoreDir, long timestamp )
        {
            return new File( getBranchedDataRootDirectory( dbStoreDir ), "" + timestamp );
        }

        public static File[] listBranchedDataDirectories( String storeDir )
        {
            return getBranchedDataRootDirectory( storeDir ).listFiles( new FileFilter()
            {
                @Override
                public boolean accept( File directory )
                {
                    return isBranchedDataDirectory( directory );
                }
            } );
        }
    }

    class LocalDatabaseOperations implements SlaveDatabaseOperations, ClusterEventReceiver
    {
        @Override
        public RequestContext getSlaveContext( int eventIdentifier )
        {
            // Constructs a slave context from scratch.
            try
            {
                XaDataSourceManager localDataSourceManager = getXaDataSourceManager();
                Collection<XaDataSource> dataSources = localDataSourceManager.getAllRegisteredDataSources();
                Tx[] txs = new Tx[dataSources.size()];
                int i = 0;
                Pair<Integer,Long> master = null;
                for ( XaDataSource dataSource : dataSources )
                {
                    long txId = dataSource.getLastCommittedTxId();
                    if( dataSource.getName().equals( Config.DEFAULT_DATA_SOURCE_NAME ) )
                    {
                        master = dataSource.getMasterForCommittedTx( txId );
                    }
                    txs[i++] = RequestContext.lastAppliedTx( dataSource.getName(), txId );
                }
                return new RequestContext( startupTime, machineId, eventIdentifier, txs, master.first(), master.other() );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public <T> T receive( Response<T> response )
        {
            try
            {
                ServerUtil.applyReceivedTransactions( response, HighlyAvailableGraphDatabase.this, ServerUtil.NO_ACTION );
                updateTime();
                return response.response();
            }
            catch ( IOException e )
            {
                newMaster( e );
                throw new RuntimeException( e );
            }
            finally
            {
                response.close();
            }
        }

        @Override
        public void handle( Exception e )
        {
            newMaster( e );
        }

        @Override
        public void exceptionHappened( RuntimeException e )
        {
            if ( e instanceof ZooKeeperException || e instanceof ComException )
            {
                slaveOperations.newMaster( e );
                throw e;
            }
        }

        @Override
        public void newMaster( Exception e )
        {
            HighlyAvailableGraphDatabase.this.newMaster( e );
        }

        /**
         * Shuts down the broker, invalidating every connection to the zookeeper
         * cluster and starts it again. Should be called in case a ConnectionExpired
         * event is received, this is the equivalent of building the ZK connection
         * from start. Also triggers a master reelect, to make sure that the state
         * ZK ended up in during our absence is respected.
         */
        @Override
        public synchronized void reconnect( Exception e )
        {
            if ( broker != null )
            {
                broker.restart();
            }
            newMaster( e );
        }

        @Override
        public int getMasterForTx( long tx )
        {
            try
            {
                return localGraph().getXaDataSourceManager().getNeoStoreDataSource().getMasterForCommittedTx( tx ).first();
            }
            catch ( IOException e )
            {
                throw new ComException( "Master id not found for tx:" + tx, e );
            }
        }
    }

    private class TxManagerCheckKernelEventHandler
        implements KernelEventHandler
    {
        @Override
        public void beforeShutdown()
        {
        }

        @Override
        public void kernelPanic( ErrorState error )
        {
            if( error == ErrorState.TX_MANAGER_NOT_OK )
            {
                messageLog.logMessage( "TxManager not ok, doing internal restart" );
                internalShutdown( true );
                newMaster( new InformativeStackTrace( "Tx manager not ok" ) );
            }
        }

        @Override
        public Object getResource()
        {
            return null;
        }

        @Override
        public ExecutionOrder orderComparedTo( KernelEventHandler other )
        {
            return ExecutionOrder.DOESNT_MATTER;
        }
    }

    private class HANodeLookup
        implements NodeProxy.NodeLookup
    {
        @Override
        public NodeImpl lookup( long nodeId )
        {
            return localGraph().getNodeManager().getNodeForProxy( nodeId, null );
        }

        @Override
        public NodeImpl lookup( long nodeId, LockType lock )
        {
            return localGraph().getNodeManager().getNodeForProxy( nodeId, lock );
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return HighlyAvailableGraphDatabase.this;
        }

        @Override
        public NodeManager getNodeManager()
        {
            return localGraph().getNodeManager();
        }
    }

    private class HARelationshipLookups
        implements RelationshipProxy.RelationshipLookups
    {
        @Override
        public Node lookupNode( long nodeId )
        {
            return localGraph().getNodeManager().getNodeById( nodeId );
        }


        @Override
        public RelationshipImpl lookupRelationship( long relationshipId )
        {
            return localGraph().getNodeManager().getRelationshipForProxy( relationshipId, null );
        }

        @Override
        public RelationshipImpl lookupRelationship( long relationshipId, LockType lock )
        {
            return localGraph().getNodeManager().getRelationshipForProxy( relationshipId, lock );
        }

        @Override
        public GraphDatabaseService getGraphDatabaseService()
        {
            return HighlyAvailableGraphDatabase.this;
        }

        @Override
        public NodeManager getNodeManager()
        {
            return localGraph().getNodeManager();
        }

        @Override
        public Node newNodeProxy( long nodeId )
        {
            return localGraph().getNodeManager().newNodeProxyById( nodeId );
        }
    }

    @Override
    public PersistenceSource getPersistenceSource()
    {
        return localGraph().getPersistenceSource();
    }

    @Override
    public Guard getGuard()
    {
        return localGraph().getGuard();
    }

    @Override
    public StoreId getStoreId()
    {
        return storeId;
    }
}
