/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Consumer;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.security.URLAccessRules;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

/**
 * Platform module for {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory}. This creates
 * all the services needed by {@link org.neo4j.kernel.impl.factory.EditionModule} implementations.
 */
public class PlatformModule
{
    public final PageCache pageCache;

    public final Monitors monitors;

    public final GraphDatabaseFacade graphDatabaseFacade;

    public final org.neo4j.kernel.impl.util.Dependencies dependencies;

    public final LogService logging;

    public final LifeSupport life;

    public final File storeDir;

    public final DiagnosticsManager diagnosticsManager;

    public final Tracers tracers;

    public final Config config;

    public final FileSystemAbstraction fileSystem;

    public final DataSourceManager dataSourceManager;

    public final KernelExtensions kernelExtensions;

    public final URLAccessRule urlAccessRule;

    public final JobScheduler jobScheduler;

    public final AvailabilityGuard availabilityGuard;

    public final TransactionCounters transactionMonitor;

    public PlatformModule( File storeDir, Map<String, String> params, final GraphDatabaseFacadeFactory.Dependencies externalDependencies,
            final GraphDatabaseFacade graphDatabaseFacade,
            OperationalMode operationalMode )
    {
        dependencies = new org.neo4j.kernel.impl.util.Dependencies( new Supplier<DependencyResolver>()
        {
            @Override
            public DependencyResolver get()
            {
                return dataSourceManager.getDataSource().getDependencyResolver();
            }
        } );
        life = dependencies.satisfyDependency( createLife() );
        this.graphDatabaseFacade = dependencies.satisfyDependency( graphDatabaseFacade );

        // SPI - provided services
        config = dependencies.satisfyDependency( new Config( params, getSettingsClasses(
                externalDependencies.settingsClasses(), externalDependencies.kernelExtensions() ) ) );

        this.storeDir = storeDir.getAbsoluteFile();

        // Database system information, used by UDC
        dependencies.satisfyDependency( new UsageData() );

        fileSystem = dependencies.satisfyDependency( createFileSystemAbstraction() );

        // Component monitoring
        monitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        dependencies.satisfyDependency( monitors );

        jobScheduler = life.add( dependencies.satisfyDependency( createJobScheduler() ) );

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logging = dependencies.satisfyDependency( createLogService( externalDependencies.userLogProvider() ) );

        config.setLogger( logging.getInternalLog( Config.class ) );

        life.add( dependencies.satisfyDependency( new StoreLockerLifecycleAdapter( new StoreLocker( fileSystem ), storeDir ) ));

        new JvmChecker( logging.getInternalLog( JvmChecker.class ),
                new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        String desiredImplementationName = config.get( GraphDatabaseFacadeFactory.Configuration.tracer );
        tracers = dependencies.satisfyDependency(
                new Tracers( desiredImplementationName, logging.getInternalLog( Tracers.class ) ) );
        dependencies.satisfyDependency( tracers.pageCacheTracer );
        dependencies.satisfyDependency( firstImplementor(
                LogRotationMonitor.class, tracers.transactionTracer, LogRotationMonitor.NULL ) );
        dependencies.satisfyDependency( firstImplementor(
                CheckPointerMonitor.class, tracers.checkPointTracer, CheckPointerMonitor.NULL ) );

        pageCache = dependencies.satisfyDependency( createPageCache( fileSystem, config, logging, tracers ) );
        life.add( new PageCacheLifecycle( pageCache ) );

        diagnosticsManager = life.add( dependencies.satisfyDependency( new DiagnosticsManager( logging.getInternalLog(
                DiagnosticsManager.class ) ) ) );

        // TODO please fix the bad dependencies instead of doing this. Before the removal of JTA
        // this was the place of the XaDataSourceManager. NeoStoreXaDataSource is create further down than
        // (specifically) KernelExtensions, which creates an interesting out-of-order issue with #doAfterRecovery().
        // Anyways please fix this.
        dataSourceManager = life.add( dependencies.satisfyDependency( new DataSourceManager() ) );

        availabilityGuard = dependencies.satisfyDependency( new AvailabilityGuard( Clock.SYSTEM_CLOCK,
                logging.getInternalLog( AvailabilityGuard.class ) ) );

        transactionMonitor = dependencies.satisfyDependency( createTransactionCounters() );

        KernelContext kernelContext = dependencies.satisfyDependency(
                new SimpleKernelContext( this.fileSystem, this.storeDir, operationalMode ));

        kernelExtensions = dependencies.satisfyDependency( new KernelExtensions(
                kernelContext,
                externalDependencies.kernelExtensions(),
                dependencies,
                UnsatisfiedDependencyStrategies.fail() ) );

        urlAccessRule = dependencies.satisfyDependency( URLAccessRules.combined( externalDependencies.urlAccessRules() ) );

        publishPlatformInfo( dependencies.resolveDependency( UsageData.class ) );
    }

    @SuppressWarnings( "unchecked" )
    private <T> T firstImplementor( Class<T> type, Object... candidates )
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

    private void publishPlatformInfo( UsageData sysInfo )
    {
        sysInfo.set( UsageDataKeys.version, Version.getKernel().getReleaseVersion() );
        sysInfo.set( UsageDataKeys.revision, Version.getKernel().getVersion() );
        sysInfo.set( UsageDataKeys.operationalMode, OperationalMode.ha );
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
        long internalLogRotationDelay = config.get( GraphDatabaseSettings.store_internal_log_rotation_delay );
        int internalLogMaxArchives = config.get( GraphDatabaseSettings.store_internal_log_max_archives );

        final StoreLogService.Builder builder =
                StoreLogService.withRotation( internalLogRotationThreshold, internalLogRotationDelay,
                        internalLogMaxArchives, jobScheduler );

        if ( userLogProvider != null )
        {
            builder.withUserLogProvider( userLogProvider );
        }

        builder.withRotationListener( new Consumer<LogProvider>()
        {
            @Override
            public void accept( LogProvider logProvider )
            {
                diagnosticsManager.dumpAll( logProvider.getLog( DiagnosticsManager.class ) );
            }
        } );

        for ( String debugContext : config.get( GraphDatabaseSettings.store_internal_debug_contexts ) )
        {
            builder.withLevel( debugContext, Level.DEBUG );
        }
        builder.withDefaultLevel( config.get( GraphDatabaseSettings.store_internal_log_level ) );

        File internalLog = config.get( GraphDatabaseSettings.store_internal_log_location );
        StoreLogService logService;
        try
        {
            if ( internalLog == null )
            {
                logService = builder.inStoreDirectory( fileSystem, storeDir );
            }
            else
            {
                logService = builder.toFile( fileSystem, internalLog );
            }
        }
        catch ( IOException ex )
        {
            throw new RuntimeException( ex );
        }
        return life.add( logService );
    }

    protected Neo4jJobScheduler createJobScheduler()
    {
        return new Neo4jJobScheduler();
    }

    protected PageCache createPageCache( FileSystemAbstraction fileSystem,
            Config config,
            LogService logging,
            Tracers tracers )
    {
        Log pageCacheLog = logging.getInternalLog( PageCache.class );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, tracers.pageCacheTracer, pageCacheLog );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        if ( config.get( GraphDatabaseSettings.dump_configuration ) )
        {
            pageCacheFactory.dumpConfiguration();
        }
        return pageCache;
    }

    protected TransactionCounters createTransactionCounters()
    {
        return new TransactionCounters();
    }

    private Iterable<Class<?>> getSettingsClasses( Iterable<Class<?>> settingsClasses,
            Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        List<Class<?>> totalSettingsClasses = Iterables.toList( settingsClasses );

        // Get the list of settings classes for extensions
        for ( KernelExtensionFactory<?> kernelExtension : kernelExtensions )
        {
            if ( kernelExtension.getSettingsClass() != null )
            {
                totalSettingsClasses.add( kernelExtension.getSettingsClass() );
            }
        }

        return totalSettingsClasses;
    }
}
