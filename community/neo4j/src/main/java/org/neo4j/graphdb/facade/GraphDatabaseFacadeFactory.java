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
package org.neo4j.graphdb.facade;

import java.io.File;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.bolt.BoltServer;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.DataCollectorManager;
import org.neo4j.graphdb.factory.module.DataSourceModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.AvailabilityGuardInstaller;
import org.neo4j.kernel.availability.StartupWaiter;
import org.neo4j.kernel.builtinprocs.SpecialBuiltInProcedures;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.dbms.NonTransactionalDbmsOperations;
import org.neo4j.kernel.impl.cache.VmPauseMonitorComponent;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.pagecache.PublishPageCacheTracerMetricsAfterStart;
import org.neo4j.kernel.impl.proc.ProcedureConfig;
import org.neo4j.kernel.impl.proc.ProcedureTransactionProvider;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.proc.TerminationGuardProvider;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.procedure.ProcedureTransaction;
import org.neo4j.scheduler.DeferredExecutor;
import org.neo4j.scheduler.Group;

import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.api.proc.Context.DATABASE_API;
import static org.neo4j.kernel.api.proc.Context.DEPENDENCY_RESOLVER;
import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;
import static org.neo4j.kernel.api.proc.Context.SECURITY_CONTEXT;

/**
 * This is the main factory for creating database instances. It delegates creation to three different modules
 * ({@link PlatformModule}, {@link AbstractEditionModule}, and {@link DataSourceModule}),
 * which create all the specific services needed to run a graph database.
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

        /**
         * Collection of command executors to start running once the db is started
         */
        Iterable<Pair<DeferredExecutor,Group>> deferredExecutors();

        /**
         * Simple callback for providing a global availability guard to top level components
         * once it is created by {@link GraphDatabaseFacadeFactory#initFacade(File, Config, Dependencies, GraphDatabaseFacade)}.
         * By default this callback is a no-op
         */
        default AvailabilityGuardInstaller availabilityGuardInstaller()
        {
            return availabilityGuard -> {};
        }
    }

    protected final DatabaseInfo databaseInfo;
    private final Function<PlatformModule,AbstractEditionModule> editionFactory;

    public GraphDatabaseFacadeFactory( DatabaseInfo databaseInfo,
            Function<PlatformModule,AbstractEditionModule> editionFactory )
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
        PlatformModule platform = createPlatform( storeDir, config, dependencies );
        AbstractEditionModule edition = editionFactory.apply( platform );
        dependencies.availabilityGuardInstaller()
                .install( edition.getGlobalAvailabilityGuard( platform.clock, platform.logging, platform.config ) );

        platform.life.add( new VmPauseMonitorComponent( config, platform.logging.getInternalLog( VmPauseMonitorComponent.class ), platform.jobScheduler ) );

        Procedures procedures = setupProcedures( platform, edition, graphDatabaseFacade );
        platform.dependencies.satisfyDependency( new NonTransactionalDbmsOperations( procedures ) );

        Logger msgLog = platform.logging.getInternalLog( getClass() ).infoLogger();
        DatabaseManager databaseManager = edition.createDatabaseManager( graphDatabaseFacade, platform, edition, procedures, msgLog );
        platform.life.add( databaseManager );
        platform.dependencies.satisfyDependency( databaseManager );

        DataCollectorManager dataCollectorManager =
                new DataCollectorManager( platform.dataSourceManager,
                                          platform.jobScheduler,
                                          procedures,
                                          platform.monitors,
                                          platform.config );
        platform.life.add( dataCollectorManager );

        edition.createSecurityModule( platform, procedures );
        SecurityProvider securityProvider = edition.getSecurityProvider();
        platform.dependencies.satisfyDependencies( securityProvider.authManager() );
        platform.dependencies.satisfyDependencies( securityProvider.userManagerSupplier() );

        platform.life.add( platform.globalKernelExtensions );
        platform.life.add( createBoltServer( platform, edition, databaseManager ) );
        platform.dependencies.satisfyDependency( edition.globalTransactionCounter() );
        platform.life.add( new PublishPageCacheTracerMetricsAfterStart( platform.tracers.pageCursorTracerSupplier ) );
        platform.life.add(
                new StartupWaiter( edition.getGlobalAvailabilityGuard( platform.clock, platform.logging, platform.config ),
                        edition.getTransactionStartTimeout() ) );
        platform.dependencies.satisfyDependency( edition.getSchemaWriteGuard() );
        platform.life.setLast( platform.eventHandlers );

        edition.createDatabases( databaseManager, config );

        String activeDatabase = config.get( GraphDatabaseSettings.active_database );
        GraphDatabaseFacade databaseFacade = databaseManager.getDatabaseFacade( activeDatabase ).orElseThrow(
                () -> new IllegalStateException( String.format( "Database %s not found. Please check the logs for startup errors.", activeDatabase ) ) );

        RuntimeException error = null;
        try
        {
            platform.life.start();
        }
        catch ( final Throwable throwable )
        {
            error = new RuntimeException( "Error starting " + getClass().getName() + ", " +
                    platform.storeLayout.storeDirectory(), throwable );
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

        return databaseFacade;
    }

    /**
     * Create the platform module. Override to replace with custom module.
     */
    protected PlatformModule createPlatform( File storeDir, Config config, final Dependencies dependencies )
    {
        return new PlatformModule( storeDir, config, databaseInfo, dependencies );
    }

    private static Procedures setupProcedures( PlatformModule platform, AbstractEditionModule editionModule, GraphDatabaseFacade facade )
    {
        File pluginDir = platform.config.get( GraphDatabaseSettings.plugin_dir );
        Log internalLog = platform.logging.getInternalLog( Procedures.class );

        ProcedureConfig procedureConfig = new ProcedureConfig( platform.config );
        Procedures procedures =
                new Procedures( facade, new SpecialBuiltInProcedures( Version.getNeo4jVersion(), platform.databaseInfo.edition.toString() ), pluginDir,
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
        procedures.registerComponent( GraphDatabaseAPI.class, ctx -> ctx.get( DATABASE_API ), false );

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

    private static BoltServer createBoltServer( PlatformModule platform, AbstractEditionModule edition, DatabaseManager databaseManager )
    {
        return new BoltServer( databaseManager, platform.jobScheduler,
                platform.connectorPortRegister, edition.getConnectionTracker(), platform.usageData, platform.config, platform.clock, platform.monitors,
                platform.logging, platform.dependencies );
    }
}
