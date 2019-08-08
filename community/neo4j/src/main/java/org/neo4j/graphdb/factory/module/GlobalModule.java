/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemLifecycleAdapter;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.GuardVersionContextSupplier;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.GlobalExtensions;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.impl.cache.VmPauseMonitorComponent;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
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
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.GlobalLockerService;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.kernel.internal.locker.LockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.monitoring.CompositeDatabaseHealth;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.DeferredExecutor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;

/**
 * Global module for {@link DatabaseManagementServiceFactory}. This creates all global services and components from DBMS.
 */
public class GlobalModule
{
    private final PageCache pageCache;
    private final Monitors globalMonitors;
    private final Dependencies globalDependencies;
    private final LogService logService;
    private final LifeSupport globalLife;
    private final StoreLayout storeLayout;
    private final DatabaseInfo databaseInfo;
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
    private final CompositeDatabaseHealth globalHealthService;
    private final FileSystemWatcherService fileSystemWatcher;
    private final DatabaseEventListeners databaseEventListeners;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final ThreadToStatementContextBridge threadToTransactionBridge;
    // In the future this may not be a global decision, but for now this is a good central place to make the decision about which storage engine to use
    private final StorageEngineFactory storageEngineFactory;
    private final DependencyResolver externalDependencyResolver;
    private final FileLockerService fileLockerService;

    public GlobalModule( File providedStoreDir, Config globalConfig, DatabaseInfo databaseInfo,
            ExternalDependencies externalDependencies )
    {
        externalDependencyResolver = externalDependencies.dependencies() != null ? externalDependencies.dependencies() : new Dependencies();

        this.databaseInfo = databaseInfo;

        globalDependencies = new Dependencies();
        globalDependencies.satisfyDependency( databaseInfo );

        globalClock = globalDependencies.satisfyDependency( createClock() );
        globalLife = createLife();

        File storeDirectory = globalConfig.isExplicitlySet( databases_root_path ) ? globalConfig.get( databases_root_path ).toFile() : providedStoreDir;
        this.storeLayout = StoreLayout.of( storeDirectory, of( globalConfig ) );

        this.globalConfig = globalDependencies.satisfyDependency( globalConfig );

        fileSystem = globalDependencies.satisfyDependency( createFileSystemAbstraction() );
        globalLife.add( new FileSystemLifecycleAdapter( fileSystem ) );

        // Component monitoring
        globalMonitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        globalDependencies.satisfyDependency( globalMonitors );

        JobScheduler jobScheduler = tryResolveOrCreate( JobScheduler.class, this::createJobScheduler );
        this.jobScheduler = globalLife.add( globalDependencies.satisfyDependency( jobScheduler ) );
        startDeferredExecutors( this.jobScheduler, externalDependencies.deferredExecutors() );

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logService = globalDependencies.satisfyDependency( createLogService( externalDependencies.userLogProvider() ) );

        globalConfig.setLogger( logService.getInternalLog( Config.class ) );

        fileLockerService = createFileLockerService();
        Locker storeLocker = fileLockerService.createStoreLocker( fileSystem, storeLayout );
        globalLife.add( globalDependencies.satisfyDependency( new LockerLifecycleAdapter( storeLocker ) ) );

        new JvmChecker( logService.getInternalLog( JvmChecker.class ),
                new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        globalLife.add( new VmPauseMonitorComponent( globalConfig, logService.getInternalLog( VmPauseMonitorComponent.class ), this.jobScheduler ) );

        globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard( globalClock );
        globalDependencies.satisfyDependency( globalAvailabilityGuard );
        globalLife.setLast( globalAvailabilityGuard );

        globalHealthService = new CompositeDatabaseHealth();
        globalDependencies.satisfyDependency( globalHealthService );

        String desiredImplementationName = globalConfig.get( GraphDatabaseSettings.tracer );
        tracers = globalDependencies.satisfyDependency( new Tracers( desiredImplementationName,
                logService.getInternalLog( Tracers.class ), globalMonitors, this.jobScheduler, globalClock ) );
        globalDependencies.satisfyDependency( tracers.getPageCacheTracer() );

        collectionsFactorySupplier = createCollectionsFactorySupplier( globalConfig, globalLife );

        pageCache = tryResolveOrCreate( PageCache.class,
                () -> createPageCache( fileSystem, globalConfig, logService, tracers, this.jobScheduler ) );

        globalLife.add( new PageCacheLifecycle( pageCache ) );

        dbmsDiagnosticsManager = new DbmsDiagnosticsManager( globalDependencies, logService );
        globalDependencies.satisfyDependency( dbmsDiagnosticsManager );

        dbmsDiagnosticsManager.dumpSystemDiagnostics();

        fileSystemWatcher = createFileSystemWatcherService( fileSystem, logService, this.jobScheduler, globalConfig );
        globalLife.add( fileSystemWatcher );
        globalDependencies.satisfyDependency( fileSystemWatcher );

        extensionFactories = externalDependencies.extensions();
        globalExtensions = globalDependencies.satisfyDependency(
                new GlobalExtensions( new GlobalExtensionContext( storeLayout, databaseInfo, globalDependencies ), extensionFactories, globalDependencies,
                        ExtensionFailureStrategies.fail() ) );

        globalDependencies.satisfyDependency( URLAccessRules.combined( externalDependencies.urlAccessRules() ) );

        databaseEventListeners = new DatabaseEventListeners( logService.getInternalLog( DatabaseEventListeners.class ) );
        Iterable<? extends DatabaseEventListener> externalListeners = externalDependencies.databaseEventListeners();
        for ( DatabaseEventListener databaseListener : externalListeners )
        {
            databaseEventListeners.registerDatabaseEventListener( databaseListener );
        }
        transactionEventListeners = new GlobalTransactionEventListeners();
        globalDependencies.satisfyDependency( transactionEventListeners );

        connectorPortRegister = new ConnectorPortRegister();
        globalDependencies.satisfyDependency( connectorPortRegister );

        threadToTransactionBridge = new ThreadToStatementContextBridge();
        globalDependencies.satisfyDependency( threadToTransactionBridge );

        // There's no way of actually configuring storage engine right now and this is on purpose since
        // we have neither figured out the surface, use cases nor other storage engines.
        storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        globalDependencies.satisfyDependency( storageEngineFactory );

        checkLegacyDefaultDatabase();
    }

    private <T> T tryResolveOrCreate( Class<T> clazz, Supplier<T> newInstanceMethod )
    {
        try
        {
            return externalDependencyResolver.resolveDependency( clazz );
        }
        catch ( IllegalArgumentException | UnsatisfiedDependencyException e )
        {
            return newInstanceMethod.get();
        }
    }

    private void checkLegacyDefaultDatabase()
    {
        if ( !globalConfig.isExplicitlySet( default_database ) )
        {
            DatabaseLayout defaultDatabaseLayout = storeLayout.databaseLayout( globalConfig.get( default_database ) );
            if ( storageEngineFactory.storageExists( fileSystem, defaultDatabaseLayout, pageCache ) )
            {
                return;
            }
            final String legacyDatabaseName = "graph.db";
            DatabaseLayout legacyDatabaseLayout = storeLayout.databaseLayout( legacyDatabaseName );
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

    private static void startDeferredExecutors( JobScheduler jobScheduler, Iterable<Pair<DeferredExecutor,Group>> deferredExecutors )
    {
        for ( Pair<DeferredExecutor,Group> executorGroupPair : deferredExecutors )
        {
            DeferredExecutor executor = executorGroupPair.first();
            Group group = executorGroupPair.other();
            executor.satisfyWith( jobScheduler.executor( group ) );
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
        if ( !config.get( GraphDatabaseSettings.filewatcher_enabled ) )
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
        long internalLogRotationThreshold = globalConfig.get( GraphDatabaseSettings.store_internal_log_rotation_threshold );
        long internalLogRotationDelay = globalConfig.get( GraphDatabaseSettings.store_internal_log_rotation_delay ).toMillis();
        int internalLogMaxArchives = globalConfig.get( GraphDatabaseSettings.store_internal_log_max_archives );

        final StoreLogService.Builder builder =
                StoreLogService.withRotation( internalLogRotationThreshold, internalLogRotationDelay,
                        internalLogMaxArchives, jobScheduler );

        if ( userLogProvider != null )
        {
            builder.withUserLogProvider( userLogProvider );
        }

        builder.withRotationListener(
                logProvider -> dbmsDiagnosticsManager.dumpAll( logProvider.getLog( DiagnosticsManager.class ) ) );

        builder.withLevels( asDebugLogLevels( globalConfig.get( GraphDatabaseSettings.store_internal_debug_contexts ) ) );
        builder.withDefaultLevel( globalConfig.get( GraphDatabaseSettings.store_internal_log_level ) )
               .withTimeZone( globalConfig.get( GraphDatabaseSettings.db_timezone ).getZoneId() );

        File logFile = globalConfig.get( store_internal_log_path ).toFile();
        if ( !logFile.getParentFile().exists() )
        {
            logFile.getParentFile().mkdirs();
        }
        StoreLogService logService;
        try
        {
            logService = builder.withInternalLog( logFile ).build( fileSystem );
        }
        catch ( IOException ex )
        {
            throw new RuntimeException( ex );
        }
        // Listen to changes to the dynamic log level settings.
        globalConfig.addListener( GraphDatabaseSettings.store_internal_log_level,
                ( before, after ) -> logService.setDefaultLogLevel( after ) );
        globalConfig.addListener( GraphDatabaseSettings.store_internal_debug_contexts,
                ( before, after ) -> logService.setContextLogLevels( asDebugLogLevels( after ) ) );
        return globalLife.add( logService );
    }

    private static Map<String,Level> asDebugLogLevels( List<String> strings )
    {
        return strings.stream().collect( toMap( identity(), s -> Level.DEBUG ) );
    }

    protected JobScheduler createJobScheduler()
    {
        return JobSchedulerFactory.createInitialisedScheduler();
    }

    protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogService logging,
            Tracers tracers, JobScheduler jobScheduler )
    {
        Log pageCacheLog = logging.getInternalLog( PageCache.class );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory( fileSystem, config, tracers.getPageCacheTracer(),
                tracers.getPageCursorTracerSupplier(), pageCacheLog,
                GuardVersionContextSupplier.INSTANCE, jobScheduler );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        if ( config.get( GraphDatabaseSettings.dump_configuration ) )
        {
            pageCacheFactory.dumpConfiguration();
        }
        return pageCache;
    }

    private static CollectionsFactorySupplier createCollectionsFactorySupplier( Config config, LifeSupport life )
    {
        final GraphDatabaseSettings.TransactionStateMemoryAllocation allocation = config.get( GraphDatabaseSettings.tx_state_memory_allocation );
        switch ( allocation )
        {
        case ON_HEAP:
            return CollectionsFactorySupplier.ON_HEAP;
        case OFF_HEAP:
            final CachingOffHeapBlockAllocator allocator = new CachingOffHeapBlockAllocator(
                    config.get( tx_state_off_heap_max_cacheable_block_size ),
                    config.get( tx_state_off_heap_block_cache_size ) );
            final OffHeapBlockAllocator sharedBlockAllocator;
            final long maxMemory = config.get( GraphDatabaseSettings.tx_state_max_off_heap_memory );
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

    public StoreLayout getStoreLayout()
    {
        return storeLayout;
    }

    public DatabaseInfo getDatabaseInfo()
    {
        return databaseInfo;
    }

    public LifeSupport getGlobalLife()
    {
        return globalLife;
    }

    public PageCache getPageCache()
    {
        return pageCache;
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

    public CompositeDatabaseHealth getGlobalHealthService()
    {
        return globalHealthService;
    }

    public DatabaseEventListeners getDatabaseEventListeners()
    {
        return databaseEventListeners;
    }

    public GlobalTransactionEventListeners getTransactionEventListeners()
    {
        return transactionEventListeners;
    }

    public StorageEngineFactory getStorageEngineFactory()
    {
        return storageEngineFactory;
    }

    public DependencyResolver getExternalDependencyResolver()
    {
        return externalDependencyResolver;
    }

    public ThreadToStatementContextBridge getThreadToTransactionBridge()
    {
        return threadToTransactionBridge;
    }

    FileLockerService getFileLockerService()
    {
        return fileLockerService;
    }
}
