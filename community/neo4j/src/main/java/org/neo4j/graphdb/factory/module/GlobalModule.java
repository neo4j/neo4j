/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphdb.factory.module;

import java.util.function.Supplier;

import org.neo4j.annotations.api.IgnoreApiCheck;
import org.neo4j.bolt.transaction.StatementProcessorTxManager;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.buffer.CentralBufferMangerHolder;
import org.neo4j.buffer.NettyMemoryManagerWrapper;
import org.neo4j.capabilities.CapabilitiesService;
import org.neo4j.capabilities.DBMSCapabilities;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.cypher.internal.util.InternalNotificationStats;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.internal.collector.RecentQueryBuffer;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.bufferpool.impl.NeoByteBufferPool;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemLifecycleAdapter;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.GlobalExtensions;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.impl.cache.VmPauseMonitorComponent;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.security.URLAccessRules;
import org.neo4j.kernel.impl.util.collection.CachingOffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.CapacityLimitingBlockAllocatorDecorator;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.collection.OffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.OffHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.watcher.DefaultFileSystemWatcherService;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.GlobalLockerService;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.kernel.internal.locker.LockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.data_collector_max_recent_query_count;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.filewatcher_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_global_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_format;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_max_archives;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;

/**
 * Global module for {@link DatabaseManagementServiceFactory}. This creates all global services and components from DBMS.
 */
@IgnoreApiCheck
public class GlobalModule
{
    private final PageCache pageCache;
    private final Monitors globalMonitors;
    private final Dependencies globalDependencies;
    private final LogService logService;
    private final LifeSupport globalLife;
    private final Neo4jLayout neo4jLayout;
    private final DbmsInfo dbmsInfo;
    private final DbmsDiagnosticsManager dbmsDiagnosticsManager;
    private final Tracers tracers;
    private final Config globalConfig;
    private final FileSystemAbstraction fileSystem;
    private final GlobalExtensions globalExtensions;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final JobScheduler jobScheduler;
    private final SystemNanoClock globalClock;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final ConnectorPortRegister connectorPortRegister;
    private final CompositeDatabaseAvailabilityGuard globalAvailabilityGuard;
    private final FileSystemWatcherService fileSystemWatcher;
    private final DatabaseEventListeners databaseEventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final DependencyResolver externalDependencyResolver;
    private final FileLockerService fileLockerService;
    private final MemoryPools memoryPools;
    private final InternalNotificationStats cypherNotificationStats;
    private final GlobalMemoryGroupTracker transactionsMemoryPool;
    private final GlobalMemoryGroupTracker otherMemoryPool;
    private final SystemGraphComponents systemGraphComponents;
    private final CentralBufferMangerHolder centralBufferMangerHolder;
    private final TransactionManager transactionManager;
    private final IOControllerService ioControllerService;
    private final CapabilitiesService capabilitiesService;

    /**
     * @param globalConfig configuration affecting global aspects of the system.
     * @param dbmsInfo the type of dbms this module manages.
     * @param externalDependencies optional external dependencies provided by caller.
     */
    public GlobalModule( Config globalConfig, DbmsInfo dbmsInfo, ExternalDependencies externalDependencies )
    {
        externalDependencyResolver = externalDependencies.dependencies() != null ? externalDependencies.dependencies() : new Dependencies();

        this.dbmsInfo = dbmsInfo;

        globalDependencies = new Dependencies();
        globalDependencies.satisfyDependency( dbmsInfo );

        globalClock = globalDependencies.satisfyDependency( createClock() );
        globalLife = createLife();

        this.neo4jLayout = Neo4jLayout.of( globalConfig );

        this.globalConfig = globalDependencies.satisfyDependency( globalConfig );

        fileSystem = tryResolveOrCreate( FileSystemAbstraction.class, this::createFileSystemAbstraction );
        globalDependencies.satisfyDependency( fileSystem );
        globalLife.add( new FileSystemLifecycleAdapter( fileSystem ) );

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logService = globalDependencies.satisfyDependency( createLogService( externalDependencies.userLogProvider() ) );
        globalConfig.setLogger( logService.getInternalLog( Config.class ) );

        // Component monitoring
        globalMonitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        globalDependencies.satisfyDependency( globalMonitors );

        JobScheduler createdOrResolvedScheduler = tryResolveOrCreate( JobScheduler.class, this::createJobScheduler );
        jobScheduler = globalLife.add( globalDependencies.satisfyDependency( createdOrResolvedScheduler ) );

        fileLockerService = createFileLockerService();
        Locker storeLocker = fileLockerService.createStoreLocker( fileSystem, neo4jLayout );
        globalLife.add( globalDependencies.satisfyDependency( new LockerLifecycleAdapter( storeLocker ) ) );

        new JvmChecker( logService.getInternalLog( JvmChecker.class ),
                new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        memoryPools = new MemoryPools( globalConfig.get( memory_tracking ) );
        otherMemoryPool = memoryPools.pool( MemoryGroup.OTHER, 0, null );
        transactionsMemoryPool =
                memoryPools.pool( MemoryGroup.TRANSACTION, globalConfig.get( memory_transaction_global_max_size ), memory_transaction_global_max_size.name() );
        globalConfig.addListener( memory_transaction_global_max_size, ( before, after ) -> transactionsMemoryPool.setSize( after ) );
        globalDependencies.satisfyDependency( memoryPools );

        centralBufferMangerHolder = crateCentralBufferManger();

        var recentQueryBuffer = new RecentQueryBuffer( globalConfig.get( data_collector_max_recent_query_count ),
                                                   memoryPools.pool( MemoryGroup.RECENT_QUERY_BUFFER, 0, null ).getPoolMemoryTracker() );
        globalDependencies.satisfyDependency( recentQueryBuffer );

        systemGraphComponents = tryResolveOrCreate( SystemGraphComponents.class, SystemGraphComponents::new );
        globalDependencies.satisfyDependency( systemGraphComponents );

        globalLife.add( new VmPauseMonitorComponent( globalConfig, logService.getInternalLog( VmPauseMonitorComponent.class ), jobScheduler, globalMonitors ) );

        globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard( globalClock );
        globalDependencies.satisfyDependency( globalAvailabilityGuard );
        globalLife.setLast( globalAvailabilityGuard );

        String desiredImplementationName = globalConfig.get( GraphDatabaseInternalSettings.tracer );
        tracers = tryResolveOrCreate(Tracers.class, () -> new Tracers( desiredImplementationName,
                logService.getInternalLog( Tracers.class ), globalMonitors, jobScheduler, globalClock, globalConfig ) );
        globalDependencies.satisfyDependency(tracers);
        globalDependencies.satisfyDependency( tracers.getPageCacheTracer() );

        collectionsFactorySupplier = createCollectionsFactorySupplier( globalConfig, globalLife );

        ioControllerService = loadIOControllerService();
        pageCache = tryResolveOrCreate( PageCache.class,
                () -> createPageCache( fileSystem, globalConfig, logService, tracers, jobScheduler, globalClock, memoryPools ) );

        globalLife.add( new PageCacheLifecycle( pageCache ) );

        dbmsDiagnosticsManager = new DbmsDiagnosticsManager( globalDependencies, logService );
        globalDependencies.satisfyDependency( dbmsDiagnosticsManager );

        dbmsDiagnosticsManager.dumpSystemDiagnostics();

        fileSystemWatcher = createFileSystemWatcherService( fileSystem, logService, jobScheduler, globalConfig );
        globalLife.add( fileSystemWatcher );
        globalDependencies.satisfyDependency( fileSystemWatcher );

        extensionFactories = externalDependencies.extensions();
        globalExtensions = globalDependencies.satisfyDependency(
                new GlobalExtensions( new GlobalExtensionContext( neo4jLayout, dbmsInfo, globalDependencies ), extensionFactories, globalDependencies,
                        ExtensionFailureStrategies.fail() ) );

        globalDependencies.satisfyDependency( URLAccessRules.combined( externalDependencies.urlAccessRules() ) );

        databaseEventListeners = new DatabaseEventListeners( logService.getInternalLog( DatabaseEventListeners.class ) );
        Iterable<? extends DatabaseEventListener> externalListeners = externalDependencies.databaseEventListeners();
        for ( DatabaseEventListener databaseListener : externalListeners )
        {
            databaseEventListeners.registerDatabaseEventListener( databaseListener );
        }
        globalDependencies.satisfyDependencies( databaseEventListeners );

        cypherNotificationStats = new InternalNotificationStats();
        globalDependencies.satisfyDependencies( cypherNotificationStats );

        transactionEventListeners = new GlobalTransactionEventListeners();
        globalDependencies.satisfyDependency( transactionEventListeners );

        connectorPortRegister = new ConnectorPortRegister();
        globalDependencies.satisfyDependency( connectorPortRegister );

        //transaction manager used for Bolt and HTTP interfaces
        transactionManager = new StatementProcessorTxManager();
        globalDependencies.satisfyDependency( transactionManager );

        capabilitiesService = loadCapabilities();
        globalDependencies.satisfyDependency( capabilitiesService );
        globalDependencies.satisfyDependency( tryResolveOrCreate( NativeAccess.class, NativeAccessProvider::getNativeAccess ) );

        checkLegacyDefaultDatabase();
    }

    private <T> T tryResolveOrCreate( Class<T> clazz, Supplier<T> newInstanceMethod )
    {
        return externalDependencyResolver.containsDependency( clazz ) ? externalDependencyResolver.resolveDependency( clazz ) : newInstanceMethod.get();
    }

    private void checkLegacyDefaultDatabase()
    {
        if ( !globalConfig.isExplicitlySet( default_database ) )
        {
            StorageEngineFactory storageEngineFactory = StorageEngineFactory.defaultStorageEngine();
            DatabaseLayout defaultDatabaseLayout = neo4jLayout.databaseLayout( globalConfig.get( default_database ) );
            if ( storageEngineFactory.storageExists( fileSystem, defaultDatabaseLayout, pageCache ) )
            {
                return;
            }
            final String legacyDatabaseName = "graph.db";
            DatabaseLayout legacyDatabaseLayout = neo4jLayout.databaseLayout( legacyDatabaseName );
            if ( storageEngineFactory.storageExists( fileSystem, legacyDatabaseLayout, pageCache ) )
            {
                Log internalLog = logService.getInternalLog( getClass() );
                globalConfig.set( default_database, legacyDatabaseName );
                internalLog.warn(
                        "Legacy `%s` database was found and default database was set to point to into it. Please consider setting default database explicitly.",
                        legacyDatabaseName );
            }
        }
    }

    protected FileLockerService createFileLockerService()
    {
        return new GlobalLockerService();
    }

    protected SystemNanoClock createClock()
    {
        return Clocks.nanoClock();
    }

    public LifeSupport createLife()
    {
        return new LifeSupport();
    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    private FileSystemWatcherService createFileSystemWatcherService( FileSystemAbstraction fileSystem, LogService logging, JobScheduler jobScheduler,
            Config config )
    {
        if ( !config.get( filewatcher_enabled ) )
        {
            Log log = logging.getInternalLog( getClass() );
            log.info( "File watcher disabled by configuration." );
            return FileSystemWatcherService.EMPTY_WATCHER;
        }

        try
        {
            return new DefaultFileSystemWatcherService( jobScheduler, fileSystem.fileWatcher() );
        }
        catch ( Exception e )
        {
            Log log = logging.getInternalLog( getClass() );
            log.warn( "Can not create file watcher for current file system. File monitoring capabilities for store files will be disabled.", e );
            return FileSystemWatcherService.EMPTY_WATCHER;
        }
    }

    protected LogService createLogService( LogProvider userLogProvider )
    {
        // Will get diagnostics as header in each newly created log file (diagnostics in the first file is printed during start up).
        Neo4jLoggerContext loggerContext =
                LogConfig.createBuilder( fileSystem, globalConfig.get( store_internal_log_path ), globalConfig.get( store_internal_log_level ) )
                         .withFormat( globalConfig.get( store_internal_log_format ) )
                .withTimezone( globalConfig.get( db_timezone ) )
                .withHeaderLogger( log -> dbmsDiagnosticsManager.dumpAll(log), DiagnosticsManager.class.getCanonicalName() )
                .withRotation( globalConfig.get( store_internal_log_rotation_threshold ), globalConfig.get( store_internal_log_max_archives ) )
                .build();
        Log4jLogProvider internalLogProvider = new Log4jLogProvider( loggerContext );
        userLogProvider = userLogProvider == null ? NullLogProvider.getInstance() : userLogProvider;
        SimpleLogService logService = new SimpleLogService( userLogProvider, internalLogProvider );

        // Listen to changes to the dynamic log level settings.
        globalConfig.addListener( store_internal_log_level,
                ( before, after ) -> internalLogProvider.updateLogLevel( after ) );

        // If the user log provider comes from us we make sure that it starts with the default log level and listens to updates.
        if ( userLogProvider instanceof Log4jLogProvider )
        {
            Log4jLogProvider provider = (Log4jLogProvider) userLogProvider;
            provider.updateLogLevel( globalConfig.get( store_internal_log_level) );
            globalConfig.addListener( store_internal_log_level,
                    ( before, after ) -> provider.updateLogLevel( after ) );
        }
        return globalLife.add( logService );
    }

    private JobScheduler createJobScheduler()
    {
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler( globalClock );
        jobScheduler.setParallelism( Group.INDEX_SAMPLING, globalConfig.get( GraphDatabaseInternalSettings.index_sampling_parallelism ) );
        jobScheduler.setParallelism( Group.INDEX_POPULATION, globalConfig.get( GraphDatabaseInternalSettings.index_population_parallelism ) );
        jobScheduler.setParallelism( Group.PAGE_CACHE_PRE_FETCHER, globalConfig.get( GraphDatabaseSettings.pagecache_scan_prefetch ) );
        return jobScheduler;
    }

    protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogService logging, Tracers tracers, JobScheduler jobScheduler,
            SystemNanoClock clock, MemoryPools memoryPools )
    {
        Log pageCacheLog = logging.getInternalLog( PageCache.class );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( fileSystem, config, tracers.getPageCacheTracer(), pageCacheLog,
                jobScheduler, clock, memoryPools );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        if ( config.get( GraphDatabaseInternalSettings.dump_configuration ) )
        {
            pageCacheFactory.dumpConfiguration();
        }
        return pageCache;
    }

    private static CollectionsFactorySupplier createCollectionsFactorySupplier( Config config, LifeSupport life )
    {
        final TransactionStateMemoryAllocation allocation = config.get( tx_state_memory_allocation );
        switch ( allocation )
        {
        case ON_HEAP:
            return CollectionsFactorySupplier.ON_HEAP;
        case OFF_HEAP:
            final CachingOffHeapBlockAllocator allocator = new CachingOffHeapBlockAllocator(
                    config.get( tx_state_off_heap_max_cacheable_block_size ),
                    config.get( tx_state_off_heap_block_cache_size ) );
            final OffHeapBlockAllocator sharedBlockAllocator;
            final long maxMemory = config.get( tx_state_max_off_heap_memory );
            if ( maxMemory > 0 )
            {
                sharedBlockAllocator = new CapacityLimitingBlockAllocatorDecorator( allocator, maxMemory );
            }
            else
            {
                sharedBlockAllocator = allocator;
            }
            life.add( onShutdown( sharedBlockAllocator::release ) );
            return () -> new OffHeapCollectionsFactory( sharedBlockAllocator );
        default:
            throw new IllegalArgumentException( "Unknown transaction state memory allocation value: " + allocation );
        }
    }

    private CentralBufferMangerHolder crateCentralBufferManger()
    {
        // since network buffers are currently the only use of the central byte buffer manager ...
        if ( !globalConfig.get( GraphDatabaseInternalSettings.managed_network_buffers ) )
        {
            return CentralBufferMangerHolder.EMPTY;
        }

        var bufferPool = new NeoByteBufferPool( memoryPools, jobScheduler );
        globalLife.add( bufferPool );
        var nettyAllocator = new NettyMemoryManagerWrapper( bufferPool );
        return new CentralBufferMangerHolder( nettyAllocator, bufferPool );
    }

    private static IOControllerService loadIOControllerService()
    {
        return Services.loadByPriority( IOControllerService.class ).orElseThrow(
                () -> new IllegalStateException( IOControllerService.class.getSimpleName() + " not found." ) );
    }

    private CapabilitiesService loadCapabilities()
    {
        var service = CapabilitiesService.newCapabilities( globalConfig, globalDependencies );
        service.set( DBMSCapabilities.dbms_instance_version, Version.getNeo4jVersion() );
        service.set( DBMSCapabilities.dbms_instance_kernel_version, Version.getKernelVersion() );
        service.set( DBMSCapabilities.dbms_instance_edition, dbmsInfo.edition.toString() );
        service.set( DBMSCapabilities.dbms_instance_operational_mode, dbmsInfo.operationalMode.toString() );
        return service;
    }

    public FileWatcher getFileWatcher()
    {
        return fileSystemWatcher.getFileWatcher();
    }

    public ConnectorPortRegister getConnectorPortRegister()
    {
        return connectorPortRegister;
    }

    CollectionsFactorySupplier getCollectionsFactorySupplier()
    {
        return collectionsFactorySupplier;
    }

    public SystemNanoClock getGlobalClock()
    {
        return globalClock;
    }

    public JobScheduler getJobScheduler()
    {
        return jobScheduler;
    }

    public GlobalExtensions getGlobalExtensions()
    {
        return globalExtensions;
    }

    Iterable<ExtensionFactory<?>> getExtensionFactories()
    {
        return extensionFactories;
    }

    public Config getGlobalConfig()
    {
        return globalConfig;
    }

    public FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    public Tracers getTracers()
    {
        return tracers;
    }

    public Neo4jLayout getNeo4jLayout()
    {
        return neo4jLayout;
    }

    public DbmsInfo getDbmsInfo()
    {
        return dbmsInfo;
    }

    public LifeSupport getGlobalLife()
    {
        return globalLife;
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public InternalNotificationStats getCypherNotificationStats()
    {
        return cypherNotificationStats;
    }
    public Monitors getGlobalMonitors()
    {
        return globalMonitors;
    }

    public Dependencies getGlobalDependencies()
    {
        return globalDependencies;
    }

    public LogService getLogService()
    {
        return logService;
    }

    public CompositeDatabaseAvailabilityGuard getGlobalAvailabilityGuard()
    {
        return globalAvailabilityGuard;
    }

    public DatabaseEventListeners getDatabaseEventListeners()
    {
        return databaseEventListeners;
    }

    public GlobalTransactionEventListeners getTransactionEventListeners()
    {
        return transactionEventListeners;
    }

    public DependencyResolver getExternalDependencyResolver()
    {
        return externalDependencyResolver;
    }

    FileLockerService getFileLockerService()
    {
        return fileLockerService;
    }

    public MemoryPools getMemoryPools()
    {
        return memoryPools;
    }

    public GlobalMemoryGroupTracker getTransactionsMemoryPool()
    {
        return transactionsMemoryPool;
    }

    public GlobalMemoryGroupTracker getOtherMemoryPool()
    {
        return otherMemoryPool;
    }

    public SystemGraphComponents getSystemGraphComponents()
    {
        return systemGraphComponents;
    }

    public CentralBufferMangerHolder getCentralBufferMangerHolder()
    {
        return centralBufferMangerHolder;
    }

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public IOControllerService getIoControllerService()
    {
        return ioControllerService;
    }

    public CapabilitiesService getCapabilitiesService()
    {
        return capabilitiesService;
    }
}
