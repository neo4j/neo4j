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

import static org.neo4j.com.SlaveContext.lastAppliedTx;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;
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

import org.neo4j.com.ComException;
import org.neo4j.com.MasterUtil;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.SlaveContext.Tx;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.BranchedDataException;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.BrokerFactory;
import org.neo4j.kernel.ha.ClusterClient;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterIdGeneratorFactory;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.MasterTxHook;
import org.neo4j.kernel.ha.MasterTxIdGenerator.MasterTxIdGeneratorFactory;
import org.neo4j.kernel.ha.ResponseReceiver;
import org.neo4j.kernel.ha.SlaveIdGenerator.SlaveIdGeneratorFactory;
import org.neo4j.kernel.ha.SlaveLockManager.SlaveLockManagerFactory;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.SlaveTxHook;
import org.neo4j.kernel.ha.SlaveTxIdGenerator.SlaveTxIdGeneratorFactory;
import org.neo4j.kernel.ha.ZooKeeperLastCommittedTxIdSetter;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.NoMasterException;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

public class HAGraphDb extends AbstractGraphDatabase
        implements GraphDatabaseService, ResponseReceiver
{
    private final String storeDir;
    private static final int STORE_COPY_RETRIES = 3;
    private static final int NEW_MASTER_STARTUP_RETRIES = 3;
    public static final String COPY_FROM_MASTER_TEMP = "temp-copy";

    private final Map<String, String> config;
    private final BrokerFactory brokerFactory;
    private final Broker broker;
    private ClusterClient clusterClient;
    private volatile EmbeddedGraphDbImpl localGraph;
    private final int machineId;
    private volatile MasterServer masterServer;
    private volatile ScheduledExecutorService updatePuller;
    private volatile long updateTime = 0;
    private volatile Throwable causeOfShutdown;

    // Is used as a session id and is updated on each internal restart
    private long startupTime;

    private final BranchedDataPolicy branchedDataPolicy;
    private final HaConfig.SlaveUpdateMode slaveUpdateMode;
    private final int readTimeout;
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

    private final StringLogger msgLog;

    /**
     * Will instantiate its own ZooKeeper broker and ClusterClient
     */
    public HAGraphDb( String storeDir, Map<String, String> config )
    {
        this( storeDir, config, null, null );
    }

    /**
     * ONLY FOR TESTING
     * Will instantiate its own ClusterClient
     */
    public HAGraphDb( String storeDir, Map<String, String> config,
            BrokerFactory brokerFactory )
    {
        this( storeDir, config, brokerFactory, null );
    }

    /**
     * ONLY FOR TESTING
     */
    public HAGraphDb( String storeDir, Map<String, String> config,
            BrokerFactory brokerFactory, ClusterClient clusterClient )
    {
        if ( config == null )
        {
            throw new IllegalArgumentException( "null config, proper configuration required" );
        }
        this.storeDir = storeDir;
        this.config = config;
        initializeTxManagerKernelPanicEventHandler();
        this.readTimeout = HaConfig.getClientReadTimeoutFromConfig( config );
        this.slaveUpdateMode = HaConfig.getSlaveUpdateModeFromConfig( config );
        this.machineId = HaConfig.getMachineIdFromConfig( config );
        this.branchedDataPolicy = HaConfig.getBranchedDataPolicyFromConfig( config );
        config.put( Config.KEEP_LOGICAL_LOGS, "true" );
        this.brokerFactory = brokerFactory != null ? brokerFactory : defaultBrokerFactory();
        this.broker = this.brokerFactory.create( this, config );
        this.msgLog = StringLogger.getLogger( storeDir );
        this.clusterClient = clusterClient != null ? clusterClient
                : defaultClusterClient();
        this.pullUpdates = false;
        startUp( HaConfig.getAllowInitFromConfig( config ) );
    }

    private void initializeTxManagerKernelPanicEventHandler()
    {
        kernelEventHandlers.add( new KernelEventHandler()
        {
            @Override public void beforeShutdown() {}

            @Override
            public void kernelPanic( ErrorState error )
            {
                if ( error == ErrorState.TX_MANAGER_NOT_OK )
                {
                    msgLog.logMessage( "TxManager not ok, doing internal restart" );
                    internalShutdown( true );
                    newMaster( new Exception( "Tx manager not ok" ) );
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
        } );
    }

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
            Pair<Master, Machine> master = clusterClient.getMasterClient();
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
                     *  Either we branched so the previous store is not there
                     *  or we did not detect a neostore file so the db is
                     *  incomplete. Either way, it is safe to delete everything.
                     */
                    BranchedDataPolicy.keep_none.handle( this );
                    copyStoreFromMaster( master );
                    moveCopiedStoreIntoWorkingDir();
                    return;
                }
                // TODO Maybe catch IOException and treat it more seriously?
                catch ( Exception e )
                {
                    msgLog.logMessage(
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
     */
    private void moveCopiedStoreIntoWorkingDir()
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
            FileUtils.moveFile( candidate, storeDir );
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
        this.msgLog.logMessage( "Cleaning database " + storeDir + " (" + branchedDataPolicy.name() +
                ") to make way for new db from master" );
        branchedDataPolicy.handle( this );
    }

    private synchronized void startUp( boolean allowInit )
    {
        msgLog.logMessage( "Starting up highly available graph database '" + storeDir + "'" );
        StoreId storeId = null;
        // TODO
        /*
         * This is kind of stupid. We need to actually check what this directory holds because
         * it might be a failed attempt from a previous copy. We should try to start a db over that
         * and if that fails (hence, something is broken) remove it, create a directory, copy from
         * master there, apply txs there, try to start a db on that and if successful copy THAT to
         * the actual working directory.
         * tl;dr - this "test" would pass if someone did "touch neostore" in the work dir. It is stupid.
         */
        if ( !new File( storeDir, NeoStore.DEFAULT_NAME ).exists() )
        {   // Try for
            long endTime = System.currentTimeMillis() + 60000;
            Exception exception = null;
            while ( System.currentTimeMillis() < endTime )
            {
                // Check if the cluster is up
                Pair<Master, Machine> master = broker.bootstrap();
                if ( master != null && !master.other().equals( Machine.NO_MACHINE ) &&
                        master.other().getMachineId() != machineId )
                {   // Join the existing cluster
                    try
                    {
                        getFreshDatabaseFromMaster( false /*branched*/);
                        msgLog.logMessage( "copied store from master" );
                        exception = null;
                        break;
                    }
                    catch ( Exception e )
                    {
                        exception = e;
                        master = broker.getMasterReally( true );
                        msgLog.logMessage( "Problems copying store from master", e );
                    }
                }
                else
                {   // I seem to be the master, the broker have created the cluster for me
                    // I'm just going to start up now
                  // storeId = broker.getClusterStoreId();
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
        newMaster( storeId, new Exception( "Starting up for the first time" ) );
        // the localGraph() below is a blocking call and is there on purpose
        localGraph();
    }

    private void checkAndRecoverCorruptLogs( EmbeddedGraphDbImpl localDb,
            boolean copiedStore )
    {
        msgLog.logMessage( "Checking for log consistency" );
        /*
         * We are going over all data sources and try to retrieve the latest transaction. If that fails then
         * the logs might be missing or corrupt. Try to recover by asking the master for the transaction and
         * either patch the current log file or recreate the missing one.
         */
        XaDataSource dataSource = localDb.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                Config.DEFAULT_DATA_SOURCE_NAME );
        msgLog.logMessage( "Checking dataSource " + dataSource.getName() );
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
            msgLog.logMessage( "Missing log version " + e.getVersion()
                               + " for transaction " + myLastCommittedTx
                               + " and datasource " + dataSource.getName() );
            corrupted = true;
            version = e.getVersion();
        }
        catch ( IOException e )
        {
            msgLog.logMessage(
                    "IO exceptions while trying to retrieve the master for the latest txid (= "
                            + myLastCommittedTx + " )", e );
        }
        catch ( RuntimeException e )
        {
            msgLog.logMessage( "Runtime exception while getting master id for"
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
                msgLog.logMessage( "Logical log file for transaction "
                                   + myLastCommittedTx + " not found." );
            }
            else
            {
                msgLog.logMessage( "Tried to extract transaction "
                                   + myLastCommittedTx
                                   + " but it was not present in the log. Trying to retrieve it from master." );
            }
            if ( copiedStore )
            {
                /*
                 *  We copied the store, so there may be pending stuff to write to disk. No point in
                 *  checking for log existence/sanity, since even if an error is detected we can
                 *  attribute it to the copy operation being in progress. Just warn then.
                 */
                msgLog.logMessage( "A store copy might be in progress. Will not act on the apparent corruption" );
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
                    msgLog.logMessage( "Log copy finished without problems" );
                }
                catch ( Exception e )
                {
                    msgLog.logMessage( "Failed to retrieve log version "
                                       + version + " from master.", e );
                }
            }
        }
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
        msgLog.logMessage( "Copying store from master" );
        String temp = getClearedTempDir().getAbsolutePath();
        Response<Void> response = master.first().copyStore( emptyContext(),
                new ToFileStoreWriter( temp ) );
        long highestLogVersion = highestLogVersion( temp );
        if ( highestLogVersion > -1 )
            NeoStore.setVersion( temp, highestLogVersion + 1 );
        EmbeddedGraphDatabase copiedDb = new EmbeddedGraphDatabase( temp,
                stringMap( KEEP_LOGICAL_LOGS, "true" ) );
        try
        {
            MasterUtil.applyReceivedTransactions( response, copiedDb, MasterUtil.txHandlerForFullCopy() );
        }
        finally
        {
            copiedDb.shutdown();
            response.close();
        }
        msgLog.logMessage( "Done copying store from master" );
    }

    private SlaveContext emptyContext()
    {
        return new SlaveContext( 0, machineId, 0, new Tx[0], 0, 0 );
    }

    /**
     * Tries to get a set of transactions for a specific data source from the
     * master and possibly write it out as a versioned log file. Useful for
     * recovering your damaged or missing log files.
     *
     * @param master The master to retrieve transactions from
     * @param datasource The datasource for which the txs to retrieve
     * @param logVersion The version of the log to rebuild, with -1 indicating
     *            apply to current one
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
            receive( response );
            return;
        }
        XaDataSource ds = localGraph().getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                datasource );
        FileChannel newLog = ( (FileSystemAbstraction) localGraph().getConfig().getParams().get(
                FileSystemAbstraction.class ) ).open(
                ds.getFileName( logVersion ), "rw" );
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

    private long highestLogVersion( String targetStoreDir )
    {
        return XaLogicalLog.getHighestHistoryLogVersion( new File( targetStoreDir ), LOGICAL_LOG_DEFAULT_NAME );
    }

    private EmbeddedGraphDbImpl localGraph()
    {
        if ( localGraph != null ) return localGraph;
        int secondsWait = Math.max( HaConfig.getClientReadTimeoutFromConfig( config )-5, 5 );
        return waitForCondition( new LocalGraphAvailableCondition(), secondsWait*1000 );
    }

    private <T,E extends Exception> T waitForCondition( Condition<T,E> condition, int timeMillis ) throws E
    {
        long endTime = System.currentTimeMillis()+timeMillis;
        T result = condition.tryToFullfill();
        while ( result == null && System.currentTimeMillis() < endTime )
        {
            sleepWithoutInterruption( 1, "Failed waiting for " + condition + " to be fulfilled" );
            result = condition.tryToFullfill();
            if ( result != null ) return result;
        }
        throw condition.failure();
    }

    private BrokerFactory defaultBrokerFactory()
    {
        return new BrokerFactory()
        {
            @Override
            public Broker create( AbstractGraphDatabase graphDb, Map<String, String> config )
            {
                return new ZooKeeperBroker( graphDb, config, HAGraphDb.this );
            }
        };
    }

    private ClusterClient defaultClusterClient()
    {
        return new ZooKeeperClusterClient(
                HaConfig.getCoordinatorsFromConfig( config ),
                HaConfig.getClusterNameFromConfig( config ), this,
                HaConfig.getZKSessionTimeoutFromConfig( config ) );
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
                    msgLog.logMessage(
                            "ZooKeeper broker returned null master" );
                    newMaster( new NullPointerException(
                            "master returned from broker" ) );
                }
                else if ( broker.getMaster().first() == null )
                {
                    newMaster( new NullPointerException(
                            "master returned from broker" ) );
                }
                receive( broker.getMaster().first().pullUpdates(
                        getSlaveContext( -1 ) ) );
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

    @Override
    public Config getConfig()
    {
        return localGraph().getConfig();
    }

    @Override
    public String getStoreDir()
    {
        return this.storeDir;
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return localGraph().getManagementBeans( type );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getStoreDir() + ", " + HaConfig.CONFIG_KEY_SERVER_ID + ":" + machineId + "]";
    }

    /**
     * Shuts down the broker, invalidating every connection to the zookeeper
     * cluster and starts it again. Should be called in case a ConnectionExpired
     * event is received, this is the equivalent of building the ZK connection
     * from start. Also triggers a master reelect, to make sure that the state
     * ZK ended up in during our absence is respected. The cluster manager is
     * not used outside of startup where this call should not happen and also it
     * doesn't keep a zoo client open - so is no reason to recreate it
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

    protected synchronized void reevaluateMyself( StoreId storeId )
    {
        Pair<Master, Machine> master = broker.getMasterReally( true );
        boolean iAmCurrentlyMaster = masterServer != null;
        msgLog.logMessage( "ReevaluateMyself: machineId=" + machineId + " with master[" + master +
                "] (I am master=" + iAmCurrentlyMaster + ", " + localGraph + ")" );
        pullUpdates = false;
        EmbeddedGraphDbImpl newDb = null;
        try
        {
            if ( master.other().getMachineId() == machineId )
            {   // I am the new master
                if ( this.localGraph == null || !iAmCurrentlyMaster )
                {   // I am currently a slave, so restart as master
                    internalShutdown( true );
                    newDb = startAsMaster( storeId );
                }
                // fire rebound event
                broker.rebindMaster();
            }
            else
            {   // Someone else is master
                broker.notifyMasterChange( master.other() );
                if ( this.localGraph == null || iAmCurrentlyMaster )
                {   // I am currently master, so restart as slave.
                    // This will result in clearing of free ids from .id files, see SlaveIdGenerator.
                    internalShutdown( true );
                    newDb = startAsSlave( storeId, master );
                }
                else
                {   // I am already a slave, so just forget the ids I got from the previous master
                    ((SlaveIdGeneratorFactory) getConfig().getIdGeneratorFactory()).forgetIdAllocationsFromMaster();
                }

            }
            if ( masterServer == null )
            {
                // The above being true means we are a slave
                instantiateAutoUpdatePullerIfConfigSaysSo();
                checkAndRecoverCorruptLogs( newDb != null ? newDb : localGraph,
                        false );
                ensureDataConsistencyWithMaster( newDb != null ? newDb
                        : localGraph, master );
                msgLog.logMessage( "Data consistent with master" );
            }
            if ( newDb != null )
            {
                doAfterLocalGraphStarted( newDb );

                // Assign the db last so that no references leak
                this.localGraph = newDb;
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

    private void safelyShutdownDb( EmbeddedGraphDbImpl newDb )
    {
        try
        {
            if ( newDb != null ) newDb.shutdown();
        }
        catch ( Exception e )
        {
            msgLog.logMessage( "Couldn't shut down newly started db", e );
        }
    }

    private void doAfterLocalGraphStarted( EmbeddedGraphDbImpl newDb )
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
        msgLog.logMessage( started, true );
        msgLog.logMessage( "--- HIGH AVAILABILITY CONFIGURATION START ---" );
        broker.logStatus( msgLog );
        msgLog.logMessage( "--- HIGH AVAILABILITY CONFIGURATION END ---", true );
    }

    private EmbeddedGraphDbImpl startAsSlave( StoreId storeId,
            Pair<Master, Machine> master )
    {
        msgLog.logMessage( "Starting[" + machineId + "] as slave", true );
        EmbeddedGraphDbImpl result = new EmbeddedGraphDbImpl( storeDir, storeId, config, this,
                new SlaveLockManagerFactory( broker, this ),
                new SlaveIdGeneratorFactory( broker, this ),
                new SlaveRelationshipTypeCreator( broker, this ),
                new SlaveTxIdGeneratorFactory( broker, this ),
                new SlaveTxHook( broker, this ),
                slaveUpdateMode.createUpdater( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
        // instantiateAutoUpdatePullerIfConfigSaysSo() moved to
        // reevaluateMyself(), after the local db has been assigned
        logHaInfo( "Started as slave" );
        this.startupTime = System.currentTimeMillis();
        return result;
    }

    private EmbeddedGraphDbImpl startAsMaster( StoreId storeId )
    {
        msgLog.logMessage( "Starting[" + machineId + "] as master", true );
        EmbeddedGraphDbImpl result = new EmbeddedGraphDbImpl( storeDir, storeId, config, this,
                CommonFactories.defaultLockManagerFactory(),
                new MasterIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                new MasterTxIdGeneratorFactory( broker ),
                new MasterTxHook(),
                new ZooKeeperLastCommittedTxIdSetter( broker ),
                CommonFactories.defaultFileSystemAbstraction() );
        this.masterServer = (MasterServer) broker.instantiateMasterServer( this );
        logHaInfo( "Started as master" );
        this.startupTime = System.currentTimeMillis();
        return result;
    }

    private void ensureDataConsistencyWithMaster( EmbeddedGraphDbImpl newDb, Pair<Master, Machine> master )
    {
        if ( master.other().getMachineId() == machineId )
        {
            msgLog.logMessage( "I am master so cannot consistency check data with master" );
            return;
        }
        else if ( master.first() == null )
        {
            // Temporarily disconnected from ZK
            RuntimeException cause = new RuntimeException( "Unable to get master from ZK" );
            shutdown( cause, false );
            throw cause;
        }

        XaDataSource nioneoDataSource = newDb.getConfig().getTxModule()
                .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        long myLastCommittedTx = nioneoDataSource.getLastCommittedTxId();
        Pair<Integer, Long> myMaster;
        try
        {
            myMaster = nioneoDataSource.getMasterForCommittedTx( myLastCommittedTx );
        }
        catch ( NoSuchLogVersionException e )
        {
            msgLog.logMessage(
                    "Logical log file for txId "
                            + myLastCommittedTx
                               + " missing [version="
                               + e.getVersion()
                               + "]. If this is startup then it will be recovered later, otherwise it might be a problem." );
            return;
        }
        catch ( IOException e )
        {
            msgLog.logMessage(
                    "Failed to get master ID for txId " + myLastCommittedTx
                            + ".", e );
            return;
        }
        catch ( Exception e )
        {
            msgLog.logMessage(
                    "Exception while getting master ID for txId "
                            + myLastCommittedTx + ".", e );
            throw new BranchedDataException( "Maybe not branched data, but it could solve it", e );
        }

        Response<Pair<Integer, Long>> response = null;
        Pair<Integer, Long> mastersMaster;
        try
        {
            response = master.first().getMasterIdForCommittedTx( myLastCommittedTx, getStoreId( newDb ) );
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
            String msg = "Branched data, I (machineId:" + machineId
                         + ") think machineId for txId (" + myLastCommittedTx
                         + ") is " + myMaster + ", but master (machineId:"
                         + master.other().getMachineId() + ") says that it's "
                         + mastersMaster;
            msgLog.logMessage( msg, true );
            RuntimeException exception = new BranchedDataException( msg );
            safelyShutdownDb( newDb );
            shutdown( exception, false );
            throw exception;
        }
        msgLog.logMessage(
                "Master id for last committed tx ok with highestTxId="
                        + myLastCommittedTx + " with masterId=" + myMaster,
                true );
    }

    private StoreId getStoreId( EmbeddedGraphDbImpl db )
    {
        XaDataSource ds = db.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                Config.DEFAULT_DATA_SOURCE_NAME );
        return ((NeoStoreXaDataSource) ds).getStoreId();
    }

    private void instantiateAutoUpdatePullerIfConfigSaysSo()
    {
        long pullInterval = HaConfig.getPullIntervalFromConfig( config );
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
                        msgLog.logMessage( "Pull updates failed", e  );
                    }
                }
            }, pullInterval, pullInterval, TimeUnit.MILLISECONDS );
        }
    }

    @Override
    public Transaction beginTx()
    {
        return localGraph().beginTx();
    }

    @Override
    public Node createNode()
    {
        return localGraph().createNode();
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return localGraph().getAllNodes();
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
    public Relationship getRelationshipById( long id )
    {
        return localGraph().getRelationshipById( id );
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return localGraph().getRelationshipTypes();
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

    public synchronized void internalShutdown( boolean rotateLogs )
    {
        msgLog.logMessage( "Internal shutdown of HA db[" + machineId + "] reference=" + this + ", masterServer=" + masterServer, new Exception( "Internal shutdown" ), true );
        pullUpdates = false;
        if ( this.updatePuller != null )
        {
            msgLog.logMessage( "Internal shutdown updatePuller", true );
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
                msgLog.logMessage(
                        "Got exception while waiting for update puller termination",
                        e, true );
            }
            msgLog.logMessage( "Internal shutdown updatePuller DONE",
                    true );
            // Do not skip this, update puller == null means it has been
            // shutdown
            this.updatePuller = null;
        }
        if ( this.masterServer != null )
        {
            msgLog.logMessage( "Internal shutdown masterServer", true );
            this.masterServer.shutdown();
            msgLog.logMessage( "Internal shutdown masterServer DONE", true );
            this.masterServer = null;
        }
        if ( this.localGraph != null )
        {
            /*
             * Commented out until this is verified that it works as expected or a better solution comes along.
             *
             * ((AbstractTransactionManager)localGraph.getConfig().getTxModule().getTxManager()).attemptWaitForTxCompletionAndBlockFutureTransactions( 7000 );
             */
            if ( rotateLogs )
            {
                for ( XaDataSource dataSource : getConfig().getTxModule().getXaDataSourceManager().getAllRegisteredDataSources() )
                {
                    try
                    {
                        dataSource.rotateLogicalLog();
                    }
                    catch ( IOException e )
                    {
                        msgLog.logMessage( "Couldn't rotate logical log for " + dataSource.getName(), e );
                    }
                }
            }
            msgLog.logMessage( "Internal shutdown localGraph", true );
            this.localGraph.shutdown();
            msgLog.logMessage( "Internal shutdown localGraph DONE", true );
            this.localGraph = null;
        }
        msgLog.flush();
        StringLogger.close( storeDir );
    }

    private synchronized void shutdown( Throwable cause, boolean shutdownBroker )
    {
        causeOfShutdown = cause;
        msgLog.logMessage( "Shutdown[" + machineId + "], " + this, true );
        if ( shutdownBroker && this.broker != null )
        {
            this.broker.shutdown();
        }
        internalShutdown( false );
    }

    @Override
    public synchronized void shutdown()
    {
        shutdown( new IllegalStateException( "shutdown called" ), true );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return localGraph().unregisterKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return localGraph().unregisterTransactionEventHandler( handler );
    }

    @Override
    public SlaveContext getSlaveContext( int eventIdentifier )
    {
        // Constructs a slave context from scratch.
        try
        {
            XaDataSourceManager localDataSourceManager =
                getConfig().getTxModule().getXaDataSourceManager();
            Collection<XaDataSource> dataSources = localDataSourceManager.getAllRegisteredDataSources();
            @SuppressWarnings("unchecked")
            Tx[] txs = new Tx[dataSources.size()];
            int i = 0;
            Pair<Integer,Long> master = null;
            for ( XaDataSource dataSource : dataSources )
            {
                long txId = dataSource.getLastCommittedTxId();
                if ( dataSource.getName().equals( Config.DEFAULT_DATA_SOURCE_NAME ) )
                    master = dataSource.getMasterForCommittedTx( txId );
                txs[i++] = lastAppliedTx( dataSource.getName(), txId );
            }
            return new SlaveContext( startupTime, machineId, eventIdentifier, txs, master.first(), master.other() );
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
            MasterUtil.applyReceivedTransactions( response, this, MasterUtil.NO_ACTION );
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
    public void newMaster( Exception e )
    {
        newMaster( null, e );
    }

    private synchronized void newMaster( StoreId storeId, Exception e )
    {
        /* MP: This is from BranchDetectingTxVerifier which can report branched data via a
         * BranchedDataException embedded inside a ComException (just to pass through the usual
         * code paths w/o any additional code). Feel free to refactor to get rid of this packing */
        if ( e instanceof ComException && e.getCause() instanceof BranchedDataException )
        {
            BranchedDataException bde = (BranchedDataException) e.getCause();
            msgLog.logMessage( "Master says I've got branched data: " + bde );
        }

        Throwable cause = null;
        int i = 0;
        boolean unexpectedException = false;
        while ( i++ < NEW_MASTER_STARTUP_RETRIES )
        {
            try
            {
                msgLog.logMessage( "newMaster called", e, true );
                reevaluateMyself( storeId );
                return;
            }
            catch ( ZooKeeperException zke )
            {
                msgLog.logMessage(
                        "ZooKeeper exception in newMaster, retry #" + i, zke );
                e = zke;
                cause = zke;
                sleepWithoutInterruption( 500, "" );
                continue;
            }
            catch ( ComException ce )
            {
                msgLog.logMessage(
                        "Communication exception in newMaster, retry #" + i, ce );
                e = ce;
                cause = ce;
                sleepWithoutInterruption( 500, "" );
                continue;
            }
            catch ( BranchedDataException bde )
            {
                msgLog.logMessage(
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
            msgLog.logMessage(
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

    int getMachineId()
    {
        return machineId;
    }

    public boolean isMaster()
    {
        return getMasterServerIfMaster() != null;
    }

    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    @Override
    public IndexManager index()
    {
        return localGraph().index();
    }

    // Only for testing purposes, simulates a network outage almost
    public void shutdownBroker()
    {
        this.broker.shutdown();
    }

    enum BranchedDataPolicy
    {
        keep_all
        {
            @Override
            void handle( HAGraphDb db )
            {
                moveAwayDb( db, branchedDataDir( db ) );
            }
        },
        keep_last
        {
            @Override
            void handle( HAGraphDb db )
            {
                File branchedDataDir = branchedDataDir( db );
                moveAwayDb( db, branchedDataDir );
                for ( File file : new File( db.storeDir ).listFiles() )
                {
                    if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                    {
                        try
                        {
                            FileUtils.deleteRecursively( file );
                        }
                        catch ( IOException e )
                        {
                            db.msgLog.logMessage( "Couldn't delete old branched data directory " + file, e );
                        }
                    }
                }
            }
        },
        keep_none
        {
            @Override
            void handle( HAGraphDb db )
            {
                for ( File file : relevantDbFiles( db ) )
                {
                    try
                    {
                        FileUtils.deleteRecursively( file );
                    }
                    catch ( IOException e )
                    {
                        db.msgLog.logMessage( "Couldn't delete file " + file, e );
                    }
                }
            }
        },
        shutdown
        {
            @Override
            void handle( HAGraphDb db )
            {
                db.shutdown();
            }
        };

        static String BRANCH_PREFIX = "branched-";

        abstract void handle( HAGraphDb db );

        protected void moveAwayDb( HAGraphDb db, File branchedDataDir )
        {
            for ( File file : relevantDbFiles( db ) )
            {
                File dest = new File( branchedDataDir, file.getName() );
                if ( !file.renameTo( dest ) ) db.msgLog.logMessage( "Couldn't move " + file.getPath() );
            }
        }

        File branchedDataDir( HAGraphDb db )
        {
            File result = new File( db.storeDir, BRANCH_PREFIX + System.currentTimeMillis() );
            result.mkdirs();
            return result;
        }

        File[] relevantDbFiles( HAGraphDb db )
        {
            return new File( db.storeDir ).listFiles( new FileFilter()
            {
                @Override
                public boolean accept( File file )
                {
                    return !file.getName().equals( StringLogger.DEFAULT_NAME ) && !isBranchedDataDirectory( file );
                }
            } );
        }

        boolean isBranchedDataDirectory( File file )
        {
            return file.isDirectory() && file.getName().startsWith( BRANCH_PREFIX );
        }
    }

    private interface Condition<T, E extends Exception>
    {
        T tryToFullfill();

        E failure();
    }


    private class LocalGraphAvailableCondition implements Condition<EmbeddedGraphDbImpl, RuntimeException>
    {
        @Override
        public EmbeddedGraphDbImpl tryToFullfill()
        {
            return localGraph;
        }

        public RuntimeException failure()
        {
            if ( causeOfShutdown != null )
            {
                return new RuntimeException( "Graph database not started", causeOfShutdown );
            }
            else
            {
                return new RuntimeException( "Graph database not assigned and no cause of shutdown, " +
                        "maybe not started yet or in the middle of master/slave swap?" );
            }
        }
    }
}
