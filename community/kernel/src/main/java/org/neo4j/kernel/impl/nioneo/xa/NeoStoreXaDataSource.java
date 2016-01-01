/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.Thunk;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.api.store.DiskLayer;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.GraphPropertiesImpl;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStorage;
import org.neo4j.kernel.impl.nioneo.store.Store;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandWriter;
import org.neo4j.kernel.impl.persistence.IdGenerationFailedException;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;

/**
 * A <CODE>NeoStoreXaDataSource</CODE> is a factory for
 * {@link NeoStoreXaConnection NeoStoreXaConnections}.
 * <p>
 * The {@link NioNeoDbPersistenceSource} will create a <CODE>NeoStoreXaDataSoruce</CODE>
 * and then Neo4j kernel will use it to create {@link XaConnection XaConnections} and
 * {@link XaResource XaResources} when running transactions and performing
 * operations on the graph.
 */
public class NeoStoreXaDataSource extends LogBackedXaDataSource implements NeoStoreProvider
{
    public static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";
    private NeoStoreTransactionContextSupplier neoStoreTransactionContextSupplier;

    @SuppressWarnings("deprecation")
    public static abstract class Configuration extends LogBackedXaDataSource.Configuration
    {
        public static final Setting<Boolean> read_only= GraphDatabaseSettings.read_only;
        public static final Setting<File> store_dir = InternalAbstractGraphDatabase.Configuration.store_dir;
        public static final Setting<File> neo_store = InternalAbstractGraphDatabase.Configuration.neo_store;
        public static final Setting<File> logical_log = InternalAbstractGraphDatabase.Configuration.logical_log;
    }
    public static final byte BRANCH_ID[] = UTF8.encode( "414141" );

    public static final String LOGICAL_LOG_DEFAULT_NAME = "nioneo_logical.log";
    private final StringLogger msgLog;

    private final Logging logging;
    private final AbstractTransactionManager txManager;
    private final DependencyResolver dependencyResolver;
    private final TransactionStateFactory stateFactory;

    @SuppressWarnings("deprecation")
    private final TransactionInterceptorProviders providers;
    private final TokenNameLookup tokenNameLookup;
    private final PropertyKeyTokenHolder propertyKeyTokens;
    private final LabelTokenHolder labelTokens;
    private final RelationshipTypeTokenHolder relationshipTypeTokens;
    private final PersistenceManager persistenceManager;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionEventHandlers transactionEventHandlers;
    private final StoreFactory storeFactory;
    private final XaFactory xaFactory;
    private final JobScheduler scheduler;
    private final UpdateableSchemaState updateableSchemaState;
    private final Config config;
    private final LockService locks;

    private LifeSupport life;

    private KernelAPI kernel;

    private NeoStore neoStore;
    private IndexingService indexingService;
    private SchemaIndexProvider indexProvider;
    private XaContainer xaContainer;
    private ArrayMap<Class<?>,Store> idGenerators;
    private IntegrityValidator integrityValidator;
    private NeoStoreFileListing fileListing;

    private File storeDir;
    private boolean readOnly;

    private CacheAccessBackDoor cacheAccess;
    private PersistenceCache persistenceCache;
    private SchemaCache schemaCache;

    private LabelScanStore labelScanStore;
    private final IndexingService.Monitor indexingServiceMonitor;
    private final FileSystemAbstraction fs;
    private Function<NeoStore, Function<List<LogEntry>, List<LogEntry>>> translatorFactory;
    private final StoreUpgrader storeMigrationProcess;

    private enum Diagnostics implements DiagnosticsExtractor<NeoStoreXaDataSource>
    {
        NEO_STORE_VERSIONS( "Store versions:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logVersions( log );
            }
        },
        NEO_STORE_ID_USAGE( "Id usage:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logIdUsage( log );
            }
        },
        PERSISTENCE_WINDOW_POOL_STATS( "Persistence Window Pool stats:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logAllWindowPoolStats( log );
            }

            @Override
            boolean applicable( DiagnosticsPhase phase )
            {
                return phase.isExplicitlyRequested();
            }
        };

        private final String message;

        private Diagnostics( String message )
        {
            this.message = message;
        }

        @Override
        public void dumpDiagnostics( final NeoStoreXaDataSource source, DiagnosticsPhase phase, StringLogger log )
        {
            if ( applicable( phase ) )
            {
                log.logLongMessage( message, new Visitor<StringLogger.LineLogger, RuntimeException>()
                {
                    @Override
                    public boolean visit( StringLogger.LineLogger logger )
                    {
                        dump( source, logger );
                        return false;
                    }
                }, true );
            }
        }

        boolean applicable( DiagnosticsPhase phase )
        {
            return phase.isInitialization() || phase.isExplicitlyRequested();
        }

        abstract void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log );
    }

    /**
     * Creates a <CODE>NeoStoreXaDataSource</CODE> using configuration from
     * <CODE>params</CODE>. First the map is checked for the parameter
     * <CODE>config</CODE>.
     * If that parameter exists a config file with that value is loaded (via
     * {@link Properties#load}). Any parameter that exist in the config file
     * and in the map passed into this constructor will take the value from the
     * map.
     * <p>
     * If <CODE>config</CODE> parameter is set but file doesn't exist an
     * <CODE>IOException</CODE> is thrown. If any problem is found with that
     * configuration file or Neo4j store can't be loaded an <CODE>IOException is
     * thrown</CODE>.
     *
     * Note that the tremendous number of dependencies for this class, clearly, is an architecture smell. It is part
     * of the ongoing work on introducing the Kernel API, where components that were previously spread throughout the
     * core API are now slowly accumulating in the Kernel implementation. Over time, these components should be
     * refactored into bigger components that wrap the very granular things we depend on here.
     */
    public NeoStoreXaDataSource( Config config, LockService locks, StoreFactory sf,
                                 StringLogger stringLogger, XaFactory xaFactory, TransactionStateFactory stateFactory,
                                 @SuppressWarnings("deprecation") TransactionInterceptorProviders providers,
                                 JobScheduler scheduler, Logging logging,
                                 UpdateableSchemaState updateableSchemaState,
                                 TokenNameLookup tokenNameLookup,
                                 DependencyResolver dependencyResolver, AbstractTransactionManager txManager,
                                 PropertyKeyTokenHolder propertyKeyTokens, LabelTokenHolder labelTokens,
                                 RelationshipTypeTokenHolder relationshipTypeTokens,
                                 PersistenceManager persistenceManager,
                                 SchemaWriteGuard schemaWriteGuard, TransactionEventHandlers transactionEventHandlers,
                                 IndexingService.Monitor indexingServiceMonitor, FileSystemAbstraction fs, Function
            <NeoStore, Function<List<LogEntry>, List<LogEntry>>> translatorFactory, StoreUpgrader storeMigrationProcess )
    {
        super( BRANCH_ID, DEFAULT_DATA_SOURCE_NAME );
        this.config = config;
        this.stateFactory = stateFactory;
        this.tokenNameLookup = tokenNameLookup;
        this.dependencyResolver = dependencyResolver;
        this.providers = providers;
        this.scheduler = scheduler;
        this.logging = logging;
        this.txManager = txManager;
        this.propertyKeyTokens = propertyKeyTokens;
        this.labelTokens = labelTokens;
        this.relationshipTypeTokens = relationshipTypeTokens;
        this.persistenceManager = persistenceManager;
        this.schemaWriteGuard = schemaWriteGuard;
        this.transactionEventHandlers = transactionEventHandlers;
        this.indexingServiceMonitor = indexingServiceMonitor;
        this.fs = fs;
        this.translatorFactory = translatorFactory;
        this.storeMigrationProcess = storeMigrationProcess;

        readOnly = config.get( Configuration.read_only );
        msgLog = stringLogger;
        this.storeFactory = sf;
        this.xaFactory = xaFactory;
        this.updateableSchemaState = updateableSchemaState;
        this.locks = locks;
    }

    @Override
    public void init()
    {   // We do our own internal life management:
        // start() does life.init() and life.start(),
        // stop() does life.stop() and life.shutdown().
    }

    @Override
    public void start() throws IOException
    {
        life = new LifeSupport();

        readOnly = config.get( Configuration.read_only );

        storeDir = config.get( Configuration.store_dir );
        File store = config.get( Configuration.neo_store );
        storeFactory.ensureStoreExists();

        final TransactionFactory tf;
        if ( providers.shouldInterceptCommitting() )
        {
            tf = new InterceptingTransactionFactory();
        }
        else
        {
            tf = new TransactionFactory();
        }

        indexProvider = dependencyResolver.resolveDependency( SchemaIndexProvider.class,
                SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE );
        storeMigrationProcess.addParticipant( indexProvider.storeMigrationParticipant() );

        // TODO: Build a real provider map
        DefaultSchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( indexProvider );

        storeMigrationProcess.migrateIfNeeded( store.getParentFile() );

        neoStore = storeFactory.newNeoStore( store );

        neoStoreTransactionContextSupplier = new NeoStoreTransactionContextSupplier( neoStore );

        schemaCache = new SchemaCache( Collections.<SchemaRule>emptyList() );

        final NodeManager nodeManager = dependencyResolver.resolveDependency( NodeManager.class );
        Iterator<? extends Cache<?>> caches = nodeManager.caches().iterator();
        persistenceCache = new PersistenceCache(
                (AutoLoadingCache<NodeImpl>)caches.next(),
                (AutoLoadingCache<RelationshipImpl>)caches.next(), new Thunk<GraphPropertiesImpl>()
        {
            @Override
            public GraphPropertiesImpl evaluate()
            {
                return nodeManager.getGraphProperties();
            }
        }, nodeManager );
        cacheAccess = new BridgingCacheAccess( nodeManager, schemaCache, updateableSchemaState, persistenceCache );

        try
        {
            indexingService = life.add(
                    new IndexingService(
                            scheduler,
                            providerMap,
                            new NeoStoreIndexStoreView( locks, neoStore ),
                            tokenNameLookup, updateableSchemaState,
                            logging, indexingServiceMonitor ) );

            integrityValidator = new IntegrityValidator( neoStore, indexingService );

            xaContainer = xaFactory.newXaContainer(this, config.get( Configuration.logical_log ),
                    XaCommandReaderFactory.DEFAULT,
                    new XaCommandWriterFactory()
                    {
                        @Override
                        public XaCommandWriter newInstance()
                        {
                            return new PhysicalLogNeoXaCommandWriter();
                        }
                    },
                    new NeoStoreInjectedTransactionValidator( integrityValidator ), tf,
                    stateFactory, providers, readOnly, translatorFactory.apply( neoStore )  );

            labelScanStore = life.add( dependencyResolver.resolveDependency( LabelScanStoreProvider.class,
                    LabelScanStoreProvider.HIGHEST_PRIORITIZED ).getLabelScanStore() );

            fileListing = new NeoStoreFileListing( xaContainer, storeDir, labelScanStore, indexingService );

            Provider<NeoStore> neoStoreProvider = new Provider<NeoStore>()
            {
                @Override
                public NeoStore instance()
                {
                    return getNeoStore();
                }
            };

            kernel = life.add( new Kernel( txManager, propertyKeyTokens, labelTokens, relationshipTypeTokens,
                    persistenceManager, updateableSchemaState, schemaWriteGuard,
                    indexingService, nodeManager, neoStoreProvider, persistenceCache, schemaCache, providerMap, fs, config, labelScanStore,
                    new CacheLayer(
                        new DiskLayer( propertyKeyTokens, labelTokens, relationshipTypeTokens,
                            new SchemaStorage( neoStore.getSchemaStore() ), neoStoreProvider, indexingService ),
                        persistenceCache, indexingService, schemaCache, nodeManager ),
                    scheduler,
                    readOnly ));

            kernel.registerTransactionHook( transactionEventHandlers );

            life.init();

            // TODO: Why isn't this done in the init() method of the indexing service?
            if ( !readOnly )
            {
                neoStore.setRecoveredStatus( true );
                try
                {
                    indexingService.initIndexes( loadIndexRules() );
                    xaContainer.openLogicalLog();
                }
                finally
                {
                    neoStore.setRecoveredStatus( false );
                }
            }
            if ( !xaContainer.getResourceManager().hasRecoveredTransactions() )
            {
                neoStore.makeStoreOk();
            }
            else
            {
                msgLog.debug( "Waiting for TM to take care of recovered " +
                        "transactions." );
            }
            idGenerators = new ArrayMap<>( (byte)5, false, false );
            this.idGenerators.put( Node.class, neoStore.getNodeStore() );
            this.idGenerators.put( Relationship.class, neoStore.getRelationshipStore() );
            this.idGenerators.put( RelationshipType.class, neoStore.getRelationshipTypeStore() );
            this.idGenerators.put( Label.class, neoStore.getLabelTokenStore() );
            this.idGenerators.put( PropertyStore.class, neoStore.getPropertyStore() );
            this.idGenerators.put( PropertyKeyTokenRecord.class, neoStore.getPropertyKeyTokenStore() );
            setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );

            // TODO Problem here is that we don't know if recovery has been performed at this point
            // if it hasn't then no index recovery will be performed since the node store is still in
            // "not ok" state and forceGetRecord will always return place holder node records that are not in use.
            // This issue will certainly introduce index inconsistencies.
            life.start();
        }
        catch ( Throwable e )
        {   // Something unexpected happened during startup
            try
            {   // Close the neostore, so that locks are released properly
                neoStore.close();
            }
            catch ( Exception closeException )
            {
                msgLog.logMessage( "Couldn't close neostore after startup failure" );
            }
            throw Exceptions.launderedException( e );
        }
    }

    public NeoStore getNeoStore()
    {
        return neoStore;
    }

    public IndexingService getIndexService()
    {
        return indexingService;
    }

    public SchemaIndexProvider getIndexProvider()
    {
        return indexProvider;
    }

    public LabelScanStore getLabelScanStore()
    {
        return labelScanStore;
    }

    public LockService getLockService()
    {
        return locks;
    }

    @Override
    public void stop() throws IOException
    {
        try
        {
            if ( !readOnly )
            {
                forceEverything();
            }
            xaContainer.close();
            life.shutdown();
            unbindLogicalLog();
            neoStore.close();
        }
        catch ( IOException e )
        {
            msgLog.error( "Something went wrong while shutting down the store. Recovery will happen on next startup. " +
                    "Please inspect the logs for the root cause and address that before attempting to restart the database " +
                    "(for example, low disk space)." );
            throw e;
        }
        msgLog.info( "NeoStore closed" );
    }

    private void forceEverything()
    {
        neoStore.flushAll();
        indexingService.flushAll();
        labelScanStore.force();
    }

    @Override
    public void shutdown()
    {   // We do our own internal life management:
        // start() does life.init() and life.start(),
        // stop() does life.stop() and life.shutdown().
    }

    public StoreId getStoreId()
    {
        return neoStore.getStoreId();
    }

    @Override
    public NeoStoreXaConnection getXaConnection()
    {
        return new NeoStoreXaConnection( neoStore,
            xaContainer.getResourceManager(), getBranchId() );
    }

    private class TransactionFactory extends XaTransactionFactory
    {
        @Override
        public XaTransaction create( long lastCommittedTxWhenTransactionStarted, TransactionState state )
        {
            NeoStoreTransactionContext context = neoStoreTransactionContextSupplier.acquire();
            context.bind( state );
            return new NeoStoreTransaction( lastCommittedTxWhenTransactionStarted, getLogicalLog(),
                neoStore, cacheAccess, indexingService, labelScanStore, integrityValidator,
                (KernelTransactionImplementation)kernel.newTransaction(), locks, context );
        }

        @Override
        public void recoveryComplete()
        {
            msgLog.debug( "Recovery complete, "
                    + "all transactions have been resolved" );
            msgLog.debug( "Rebuilding id generators as needed. "
                    + "This can take a while for large stores..." );
            forceEverything();
            neoStore.makeStoreOk();
            neoStore.setVersion( xaContainer.getLogicalLog().getHighestLogVersion() );
            msgLog.debug( "Rebuild of id generators complete." );
        }

        @Override
        public long getCurrentVersion()
        {
            return neoStore.getVersion();
        }

        @Override
        public long getAndSetNewVersion()
        {
            return neoStore.incrementVersion();
        }

        @Override
        public void setVersion( long version )
        {
            neoStore.setVersion( version );
        }

        @Override
        public void flushAll()
        {
            forceEverything();
        }

        @Override
        public long getLastCommittedTx()
        {
            return neoStore.getLastCommittedTx();
        }
    }

    private class InterceptingTransactionFactory extends TransactionFactory
    {
        @Override
        public XaTransaction create( long lastCommittedTxWhenTransactionStarted, TransactionState state )
        {
            TransactionInterceptor first = providers.resolveChain( NeoStoreXaDataSource.this );
            NeoStoreTransactionContext context = neoStoreTransactionContextSupplier.acquire();
            context.bind( state );
            return new InterceptingWriteTransaction( lastCommittedTxWhenTransactionStarted, getLogicalLog(),
                    neoStore, cacheAccess, indexingService, labelScanStore, first, integrityValidator,
                    (KernelTransactionImplementation)kernel.newTransaction(), locks, context );
        }
    }

    public long nextId( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );

        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.nextId();
    }

    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );
        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.getHighestPossibleIdInUse();
    }

    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );
        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.getNumberOfIdsInUse();
    }

    public String getStoreDir()
    {
        return storeDir.getPath();
    }

    @Override
    public long getCreationTime()
    {
        return neoStore.getCreationTime();
    }

    @Override
    public long getRandomIdentifier()
    {
        return neoStore.getRandomNumber();
    }

    @Override
    public long getCurrentLogVersion()
    {
        return neoStore.getVersion();
    }

    public long incrementAndGetLogVersion()
    {
        return neoStore.incrementVersion();
    }

    // used for testing, do not use.
    @Override
    public void setLastCommittedTxId( long txId )
    {
        neoStore.setRecoveredStatus( true );
        try
        {
            neoStore.setLastCommittedTx( txId );
        }
        finally
        {
            neoStore.setRecoveredStatus( false );
        }
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public List<WindowPoolStats> getWindowPoolStats()
    {
        return neoStore.getAllWindowPoolStats();
    }

    @Override
    public long getLastCommittedTxId()
    {
        return neoStore.getLastCommittedTx();
    }

    @Override
    public XaContainer getXaContainer()
    {
        return xaContainer;
    }

    public KernelAPI getKernel()
    {
        return kernel;
    }

    @Override
    public boolean setRecovered( boolean recovered )
    {
        boolean currentValue = neoStore.isInRecoveryMode();
        neoStore.setRecoveredStatus( true );
        return currentValue;
    }

    @Override
    public ResourceIterator<File> listStoreFiles( boolean includeLogicalLogs ) throws IOException
    {
        return fileListing.listStoreFiles( includeLogicalLogs );
    }

    @Override
    public ResourceIterator<File> listStoreFiles() throws IOException
    {
        return fileListing.listStoreFiles();
    }

    @Override
    public ResourceIterator<File> listLogicalLogs() throws IOException
    {
        return fileListing.listLogicalLogs();
    }

    public void registerDiagnosticsWith( DiagnosticsManager manager )
    {
        manager.registerAll( Diagnostics.class, this );
    }

    private Iterator<IndexRule> loadIndexRules()
    {
        return new SchemaStorage( neoStore.getSchemaStore() ).allIndexRules();
    }

    @Override
    public NeoStore evaluate()
    {
        return neoStore;
    }

    @Override
    public void recoveryCompleted() throws IOException
    {
        indexingService.startIndexes();
    }

    @Override
    public LogBufferFactory createLogBufferFactory()
    {
        return xaContainer.getLogicalLog().createLogWriter( new Function<Config, File>()
        {
            @Override
            public File apply( Config config )
            {
                return config.get( Configuration.logical_log );
            }
        } );
    }

    /**
     * This must only be called when the database is otherwise inaccessible.
     */
    public void reloadSchemaCache()
    {
        ((Kernel) kernel).loadSchemaCache();
    }
}
