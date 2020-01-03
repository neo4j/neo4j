/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.index.internal.gbptree.GroupingRecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemLifecycleAdapter;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.extension.GlobalKernelExtensions;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensionFailureStrategies;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.security.URLAccessRules;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.collection.CachingOffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.CapacityLimitingBlockAllocatorDecorator;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.collection.OffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.OffHeapCollectionsFactory;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.info.SystemDiagnostics;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.internal.locker.GlobalStoreLocker;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.kernel.internal.locker.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.scheduler.DeferredExecutor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;

/**
 * Platform module for {@link GraphDatabaseFacadeFactory}. This creates
 * all the services needed by {@link AbstractEditionModule} implementations.
 */
public class PlatformModule
{
    public final PageCache pageCache;

    public final Monitors monitors;

    public final org.neo4j.kernel.impl.util.Dependencies dependencies;

    public final LogService logging;

    public final LifeSupport life;

    public final StoreLayout storeLayout;

    public final DatabaseInfo databaseInfo;

    public final DiagnosticsManager diagnosticsManager;

    public final KernelEventHandlers eventHandlers;
    public final DatabasePanicEventGenerator panicEventGenerator;

    public final Tracers tracers;

    public final Config config;

    public final FileSystemAbstraction fileSystem;

    public final DataSourceManager dataSourceManager;

    public final GlobalKernelExtensions globalKernelExtensions;
    public final Iterable<KernelExtensionFactory<?>> kernelExtensionFactories;
    public final Iterable<QueryEngineProvider> engineProviders;

    public final URLAccessRule urlAccessRule;

    public final JobScheduler jobScheduler;

    public final SystemNanoClock clock;

    public final VersionContextSupplier versionContextSupplier;

    public final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;

    public final CollectionsFactorySupplier collectionsFactorySupplier;

    public final UsageData usageData;

    public final ConnectorPortRegister connectorPortRegister;

    public PlatformModule( File providedStoreDir, Config config, DatabaseInfo databaseInfo,
            GraphDatabaseFacadeFactory.Dependencies externalDependencies )
    {
        this.databaseInfo = databaseInfo;
        this.dataSourceManager = new DataSourceManager( config );
        dependencies = new Dependencies();
        dependencies.satisfyDependency( databaseInfo );

        clock = dependencies.satisfyDependency( createClock() );
        life = dependencies.satisfyDependency( createLife() );

        this.storeLayout = StoreLayout.of( providedStoreDir );

        config.augmentDefaults( GraphDatabaseSettings.neo4j_home, storeLayout.storeDirectory().getPath() );
        this.config = dependencies.satisfyDependency( config );

        fileSystem = dependencies.satisfyDependency( createFileSystemAbstraction() );
        life.add( new FileSystemLifecycleAdapter( fileSystem ) );

        // Component monitoring
        monitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        dependencies.satisfyDependency( monitors );

        jobScheduler = life.add( dependencies.satisfyDependency( createJobScheduler() ) );
        startDeferredExecutors( jobScheduler, externalDependencies.deferredExecutors() );

        // Cleanup after recovery, used by GBPTree, added to life in NeoStoreDataSource
        recoveryCleanupWorkCollector = new GroupingRecoveryCleanupWorkCollector( jobScheduler );
        dependencies.satisfyDependency( recoveryCleanupWorkCollector );

        // Database system information, used by UDC
        usageData = new UsageData( jobScheduler );
        dependencies.satisfyDependency( life.add( usageData ) );

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logging = dependencies.satisfyDependency( createLogService( externalDependencies.userLogProvider() ) );

        config.setLogger( logging.getInternalLog( Config.class ) );

        life.add( dependencies
                .satisfyDependency( new StoreLockerLifecycleAdapter( createStoreLocker() ) ) );

        new JvmChecker( logging.getInternalLog( JvmChecker.class ),
                new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        String desiredImplementationName = config.get( GraphDatabaseSettings.tracer );
        tracers = dependencies.satisfyDependency( new Tracers( desiredImplementationName,
                logging.getInternalLog( Tracers.class ), monitors, jobScheduler, clock ) );
        dependencies.satisfyDependency( tracers.pageCacheTracer );
        dependencies.satisfyDependency( firstImplementor(
                LogRotationMonitor.class, tracers.transactionTracer, LogRotationMonitor.NULL ) );
        dependencies.satisfyDependency( firstImplementor(
                CheckPointerMonitor.class, tracers.checkPointTracer, CheckPointerMonitor.NULL ) );

        versionContextSupplier = createCursorContextSupplier( config );

        collectionsFactorySupplier = createCollectionsFactorySupplier( config, life );

        dependencies.satisfyDependency( versionContextSupplier );
        pageCache = dependencies.satisfyDependency( createPageCache( fileSystem, config, logging, tracers, versionContextSupplier, jobScheduler ) );

        life.add( new PageCacheLifecycle( pageCache ) );

        diagnosticsManager = life.add( dependencies
                .satisfyDependency( new DiagnosticsManager( logging.getInternalLog( DiagnosticsManager.class ) ) ) );
        SystemDiagnostics.registerWith( diagnosticsManager );

        dependencies.satisfyDependency( dataSourceManager );

        kernelExtensionFactories = externalDependencies.kernelExtensions();
        engineProviders = externalDependencies.executionEngines();
        globalKernelExtensions = dependencies.satisfyDependency(
                new GlobalKernelExtensions( new SimpleKernelContext( storeLayout.storeDirectory(), databaseInfo, dependencies ),
                        kernelExtensionFactories, dependencies, KernelExtensionFailureStrategies.fail() ) );

        urlAccessRule = dependencies.satisfyDependency( URLAccessRules.combined( externalDependencies.urlAccessRules() ) );

        connectorPortRegister = new ConnectorPortRegister();
        dependencies.satisfyDependency( connectorPortRegister );

        eventHandlers = new KernelEventHandlers( logging.getInternalLog( KernelEventHandlers.class ) );
        panicEventGenerator = new DatabasePanicEventGenerator( eventHandlers );

        publishPlatformInfo( dependencies.resolveDependency( UsageData.class ) );
    }

    private void startDeferredExecutors( JobScheduler jobScheduler, Iterable<Pair<DeferredExecutor,Group>> deferredExecutors )
    {
        for ( Pair<DeferredExecutor,Group> executorGroupPair : deferredExecutors )
        {
            DeferredExecutor executor = executorGroupPair.first();
            Group group = executorGroupPair.other();
            executor.satisfyWith( jobScheduler.executor( group ) );
        }
    }

    protected VersionContextSupplier createCursorContextSupplier( Config config )
    {
        return config.get( GraphDatabaseSettings.snapshot_query ) ? new TransactionVersionContextSupplier()
                                                                  : EmptyVersionContextSupplier.EMPTY;
    }

    protected StoreLocker createStoreLocker()
    {
        return new GlobalStoreLocker( fileSystem, storeLayout );
    }

    protected SystemNanoClock createClock()
    {
        return Clocks.nanoClock();
    }

    @SuppressWarnings( "unchecked" )
    private static <T> T firstImplementor( Class<T> type, Object... candidates )
    {
        for ( Object candidate : candidates )
        {
            if ( type.isInstance( candidate ) )
            {
                return (T) candidate;
            }
        }
        return null;
    }

    private static void publishPlatformInfo( UsageData sysInfo )
    {
        sysInfo.set( UsageDataKeys.version, Version.getNeo4jVersion() );
        sysInfo.set( UsageDataKeys.revision, Version.getKernelVersion() );
    }

    public LifeSupport createLife()
    {
        return new LifeSupport();
    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    protected LogService createLogService( LogProvider userLogProvider )
    {
        long internalLogRotationThreshold = config.get( GraphDatabaseSettings.store_internal_log_rotation_threshold );
        long internalLogRotationDelay = config.get( GraphDatabaseSettings.store_internal_log_rotation_delay ).toMillis();
        int internalLogMaxArchives = config.get( GraphDatabaseSettings.store_internal_log_max_archives );

        final StoreLogService.Builder builder =
                StoreLogService.withRotation( internalLogRotationThreshold, internalLogRotationDelay,
                        internalLogMaxArchives, jobScheduler );

        if ( userLogProvider != null )
        {
            builder.withUserLogProvider( userLogProvider );
        }

        builder.withRotationListener(
                logProvider -> diagnosticsManager.dumpAll( logProvider.getLog( DiagnosticsManager.class ) ) );

        for ( String debugContext : config.get( GraphDatabaseSettings.store_internal_debug_contexts ) )
        {
            builder.withLevel( debugContext, Level.DEBUG );
        }
        builder.withDefaultLevel( config.get( GraphDatabaseSettings.store_internal_log_level ) )
               .withTimeZone( config.get( GraphDatabaseSettings.db_timezone ).getZoneId() );

        File logFile = config.get( store_internal_log_path );
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
        return life.add( logService );
    }

    protected JobScheduler createJobScheduler()
    {
        return JobSchedulerFactory.createInitialisedScheduler();
    }

    protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogService logging,
            Tracers tracers, VersionContextSupplier versionContextSupplier, JobScheduler jobScheduler )
    {
        Log pageCacheLog = logging.getInternalLog( PageCache.class );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, tracers.pageCacheTracer, tracers.pageCursorTracerSupplier, pageCacheLog,
                versionContextSupplier, jobScheduler );
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
}
