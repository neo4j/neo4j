/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb.facade;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.bolt.BoltServer;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DataSourceModule;
import org.neo4j.graphdb.factory.module.EditionModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.StartupWaiter;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.builtinprocs.SpecialBuiltInProcedures;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.dbms.NonTransactionalDbmsOperations;
import org.neo4j.kernel.impl.cache.VmPauseMonitorComponent;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.pagecache.PublishPageCacheTracerMetricsAfterStart;
import org.neo4j.kernel.impl.proc.ProcedureConfig;
import org.neo4j.kernel.impl.proc.ProcedureTransactionProvider;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.proc.TerminationGuardProvider;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.procedure.ProcedureTransaction;

import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.api.proc.Context.DEPENDENCY_RESOLVER;
import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;
import static org.neo4j.kernel.api.proc.Context.SECURITY_CONTEXT;
import static org.neo4j.kernel.impl.query.QueryEngineProvider.noEngine;

/**
 * This is the main factory for creating database instances. It delegates creation to three different modules
 * ({@link PlatformModule}, {@link EditionModule}, and {@link DataSourceModule}),
 * which create all the specific services needed to run a graph database.
 * <p>
 * It is abstract in order for subclasses to specify their own {@link EditionModule}
 * implementations. Subclasses also have to set the edition name in overridden version of
 * {@link #initFacade(File, Map, GraphDatabaseFacadeFactory.Dependencies, GraphDatabaseFacade)},
 * which is used for logging and similar.
 * <p>
 * To create test versions of databases, override an edition factory (e.g. {@link org.neo4j.kernel.impl.factory
 * .CommunityFacadeFactory}), and replace modules
 * with custom versions that instantiate alternative services.
 */
public class GraphDatabaseFacadeFactory
{
    public interface Dependencies
    {
        /**
         * Allowed to be null. Null means that no external {@link org.neo4j.kernel.monitoring.Monitors} was created,
         * let the
         * database create its own monitors instance.
         */
        Monitors monitors();

        LogProvider userLogProvider();

        Iterable<Class<?>> settingsClasses();

        Iterable<KernelExtensionFactory<?>> kernelExtensions();

        Map<String,URLAccessRule> urlAccessRules();

        Iterable<QueryEngineProvider> executionEngines();
    }

    protected final DatabaseInfo databaseInfo;
    private final Function<PlatformModule,EditionModule> editionFactory;

    public GraphDatabaseFacadeFactory( DatabaseInfo databaseInfo,
            Function<PlatformModule,EditionModule> editionFactory )
    {
        this.databaseInfo = databaseInfo;
        this.editionFactory = editionFactory;
    }

    /**
     * Instantiate a graph database given configuration and dependencies.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param config configuration
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @return the newly constructed {@link GraphDatabaseFacade}
     */
    public GraphDatabaseFacade newFacade( File storeDir, Config config, final Dependencies dependencies )
    {
        return initFacade( storeDir, config, dependencies, new GraphDatabaseFacade() );
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of {@link org
     * .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param params configuration parameters
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @param graphDatabaseFacade the already created facade which needs initialisation
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public GraphDatabaseFacade initFacade( File storeDir, Map<String,String> params, final Dependencies dependencies,
            final GraphDatabaseFacade graphDatabaseFacade )
    {
        return initFacade( storeDir, Config.defaults( params ), dependencies, graphDatabaseFacade );
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of {@link org
     * .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param config configuration
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @param graphDatabaseFacade the already created facade which needs initialisation
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public GraphDatabaseFacade initFacade( File storeDir, Config config, final Dependencies dependencies,
            final GraphDatabaseFacade graphDatabaseFacade )
    {
        PlatformModule platform = createPlatform( storeDir, config, dependencies, graphDatabaseFacade );
        EditionModule edition = editionFactory.apply( platform );

        AtomicReference<QueryExecutionEngine> queryEngine = new AtomicReference<>( noEngine() );

        Procedures procedures = setupProcedures( platform, edition );
        platform.dependencies.satisfyDependency( new NonTransactionalDbmsOperations( procedures ) );
        edition.setupSecurityModule( platform, procedures );

        final DataSourceModule dataSource = createDataSource( platform, edition, queryEngine::get, procedures );

        platform.life.add( createBoltServer( platform ) );
        platform.life.add( new VmPauseMonitorComponent( config, platform.logging.getInternalLog( VmPauseMonitorComponent.class ), platform.jobScheduler ) );
        platform.life.add( new PublishPageCacheTracerMetricsAfterStart( platform.tracers.pageCursorTracerSupplier ) );
        DatabaseAvailability databaseAvailability = new DatabaseAvailability( platform.availabilityGuard, platform.transactionMonitor,
                config.get( GraphDatabaseSettings.shutdown_transaction_end_timeout ).toMillis() );
        platform.dependencies.satisfyDependency( databaseAvailability );
        platform.life.add( databaseAvailability );
        platform.life.add( new StartupWaiter( platform.availabilityGuard, edition.transactionStartTimeout ) );
        platform.life.setLast( platform.eventHandlers );

        Logger msgLog = platform.logging.getInternalLog( getClass() ).infoLogger();
        CoreAPIAvailabilityGuard coreAPIAvailabilityGuard = edition.coreAPIAvailabilityGuard;

        ClassicCoreSPI spi = new ClassicCoreSPI( platform, dataSource, msgLog, coreAPIAvailabilityGuard );
        graphDatabaseFacade.init(
                spi,
                dataSource.threadToTransactionBridge,
                platform.config,
                dataSource.tokenHolders
        );

        // Start it
        platform.dataSourceManager.addListener( new DataSourceManager.Listener()
        {
            private QueryExecutionEngine engine;

            @Override
            public void registered( NeoStoreDataSource dataSource )
            {
                if ( engine == null )
                {
                    engine = QueryEngineProvider.initialize(
                            platform.dependencies, platform.graphDatabaseFacade, dependencies.executionEngines()
                    );
                }

                queryEngine.set( engine );
            }

            @Override
            public void unregistered( NeoStoreDataSource dataSource )
            {
                queryEngine.set( noEngine() );
            }
        } );

        RuntimeException error = null;
        try
        {
            // Done after create to avoid a redundant
            // "database is now unavailable"
            enableAvailabilityLogging( platform.availabilityGuard, msgLog );

            platform.life.start();
        }
        catch ( final Throwable throwable )
        {
            error = new RuntimeException( "Error starting " + getClass().getName() + ", " +
                    platform.storeDir, throwable );
        }
        finally
        {
            if ( error != null )
            {
                try
                {
                    graphDatabaseFacade.shutdown();
                }
                catch ( Throwable shutdownError )
                {
                    error.addSuppressed( shutdownError );
                }
            }
        }

        if ( error != null )
        {
            msgLog.log( "Failed to start database", error );
            throw error;
        }

        return graphDatabaseFacade;
    }

    /**
     * Create the platform module. Override to replace with custom module.
     */
    protected PlatformModule createPlatform( File storeDir, Config config, final Dependencies dependencies,
            final GraphDatabaseFacade graphDatabaseFacade )
    {
        return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade );
    }

    /**
     * Create the datasource module. Override to replace with custom module.
     */
    protected DataSourceModule createDataSource( PlatformModule platformModule, EditionModule editionModule, Supplier<QueryExecutionEngine> queryEngine,
            Procedures procedures )
    {
        return new DataSourceModule( platformModule, editionModule, queryEngine, procedures );
    }

    private void enableAvailabilityLogging( AvailabilityGuard availabilityGuard, final Logger msgLog )
    {
        availabilityGuard.addListener( new AvailabilityGuard.AvailabilityListener()
        {
            @Override
            public void available()
            {
                msgLog.log( "Database is now ready" );
            }

            @Override
            public void unavailable()
            {
                msgLog.log( "Database is now unavailable" );
            }
        } );
    }

    private static Procedures setupProcedures( PlatformModule platform, EditionModule editionModule )
    {
        File pluginDir = platform.config.get( GraphDatabaseSettings.plugin_dir );
        Log internalLog = platform.logging.getInternalLog( Procedures.class );
        EmbeddedProxySPI proxySPI = platform.dependencies.resolveDependency( EmbeddedProxySPI.class );

        ProcedureConfig procedureConfig = new ProcedureConfig( platform.config );
        Procedures procedures =
                new Procedures( proxySPI, new SpecialBuiltInProcedures( Version.getNeo4jVersion(), platform.databaseInfo.edition.toString() ), pluginDir,
                        internalLog, procedureConfig );
        platform.life.add( procedures );
        platform.dependencies.satisfyDependency( procedures );

        procedures.registerType( Node.class, NTNode );
        procedures.registerType( Relationship.class, NTRelationship );
        procedures.registerType( Path.class, NTPath );
        procedures.registerType( Geometry.class, NTGeometry );
        procedures.registerType( Point.class, NTPoint );

        // Register injected public API components
        Log proceduresLog = platform.logging.getUserLog( Procedures.class );
        procedures.registerComponent( Log.class, ctx -> proceduresLog, true );

        procedures.registerComponent( ProcedureTransaction.class, new ProcedureTransactionProvider(), true );
        procedures.registerComponent( org.neo4j.procedure.TerminationGuard.class, new TerminationGuardProvider(), true );

        // Below components are not public API, but are made available for internal
        // procedures to call, and to provide temporary workarounds for the following
        // patterns:
        //  - Batch-transaction imports (GDAPI, needs to be real and passed to background processing threads)
        //  - Group-transaction writes (same pattern as above, but rather than splitting large transactions,
        //                              combine lots of small ones)
        //  - Bleeding-edge performance (KernelTransaction, to bypass overhead of working with Core API)
        procedures.registerComponent( DependencyResolver.class, ctx -> ctx.get( DEPENDENCY_RESOLVER ), false );
        procedures.registerComponent( KernelTransaction.class, ctx -> ctx.get( KERNEL_TRANSACTION ), false );
        procedures.registerComponent( GraphDatabaseAPI.class, ctx -> platform.graphDatabaseFacade, false );

        // Security procedures
        procedures.registerComponent( SecurityContext.class, ctx -> ctx.get( SECURITY_CONTEXT ), true );

        // Edition procedures
        try
        {
            editionModule.registerProcedures( procedures, procedureConfig );
        }
        catch ( KernelException e )
        {
            internalLog.error( "Failed to register built-in edition procedures at start up: " + e.getMessage() );
        }

        return procedures;
    }

    private static BoltServer createBoltServer( PlatformModule platform )
    {
        return new BoltServer( platform.graphDatabaseFacade, platform.fileSystem, platform.jobScheduler, platform.availabilityGuard,
                platform.connectorPortRegister, platform.usageData, platform.config, platform.clock, platform.monitors, platform.logging,
                platform.dependencies );
    }
}
