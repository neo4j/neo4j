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
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.collector.DataCollector;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.StartupWaiter;
import org.neo4j.kernel.builtinprocs.SpecialBuiltInProcedures;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.api.dbms.NonTransactionalDbmsOperations;
import org.neo4j.kernel.impl.cache.VmPauseMonitorComponent;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.pagecache.PublishPageCacheTracerMetricsAfterStart;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.impl.proc.ProcedureConfig;
import org.neo4j.kernel.impl.proc.ProcedureTransactionProvider;
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
 * ({@link PlatformModule}, {@link AbstractEditionModule}, and {@link DatabaseModule}),
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

        Iterable<ExtensionFactory<?>> extensions();

        Map<String,URLAccessRule> urlAccessRules();

        Iterable<QueryEngineProvider> executionEngines();

        /**
         * Collection of command executors to start running once the db is started
         */
        Iterable<Pair<DeferredExecutor,Group>> deferredExecutors();
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

        platform.life.add( new VmPauseMonitorComponent( config, platform.logService.getInternalLog( VmPauseMonitorComponent.class ), platform.jobScheduler ) );

        GlobalProcedures globalProcedures = setupProcedures( platform, edition );
        platform.dependencies.satisfyDependency( new NonTransactionalDbmsOperations( globalProcedures ) );

        Logger logger = platform.logService.getInternalLog( getClass() ).infoLogger();
        DatabaseManager databaseManager = createAndInitializeDatabaseManager( platform, edition, graphDatabaseFacade, globalProcedures, logger );

        edition.createSecurityModule( platform, globalProcedures );
        SecurityProvider securityProvider = edition.getSecurityProvider();
        platform.dependencies.satisfyDependencies( securityProvider.authManager() );
        platform.dependencies.satisfyDependencies( securityProvider.userManagerSupplier() );

        platform.life.add( platform.globalExtensions );
        platform.life.add( createBoltServer( platform, edition, databaseManager ) );
        platform.dependencies.satisfyDependency( edition.globalTransactionCounter() );
        platform.life.add( new PublishPageCacheTracerMetricsAfterStart( platform.tracers.getPageCursorTracerSupplier() ) );
        platform.life.add(
                new StartupWaiter( edition.getGlobalAvailabilityGuard( platform.clock, platform.logService, platform.config ),
                        edition.getTransactionStartTimeout() ) );
        platform.dependencies.satisfyDependency( edition.getSchemaWriteGuard() );

        RuntimeException error = null;
        GraphDatabaseFacade databaseFacade = null;
        try
        {
            edition.createDatabases( databaseManager, config );
            platform.life.start();
            String activeDatabase = config.get( GraphDatabaseSettings.active_database );
            databaseFacade = databaseManager.getDatabaseContext( activeDatabase ).orElseThrow( () -> new IllegalStateException(
                    String.format( "Database %s not found. Please check the logs for startup errors.", activeDatabase ) ) ).getDatabaseFacade();

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
            logger.log( "Failed to start database", error );
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

    private static GlobalProcedures setupProcedures( PlatformModule platform, AbstractEditionModule editionModule )
    {
        File proceduresDirectory = platform.config.get( GraphDatabaseSettings.plugin_dir );
        Log internalLog = platform.logService.getInternalLog( GlobalProcedures.class );
        Log proceduresLog = platform.logService.getUserLog( GlobalProcedures.class );

        ProcedureConfig procedureConfig = new ProcedureConfig( platform.config );
        SpecialBuiltInProcedures builtInProcedures = new SpecialBuiltInProcedures( Version.getNeo4jVersion(), platform.databaseInfo.edition.toString() );
        GlobalProcedures globalProcedures = new GlobalProcedures( builtInProcedures, proceduresDirectory, internalLog, procedureConfig );

        globalProcedures.registerType( Node.class, NTNode );
        globalProcedures.registerType( Relationship.class, NTRelationship );
        globalProcedures.registerType( Path.class, NTPath );
        globalProcedures.registerType( Geometry.class, NTGeometry );
        globalProcedures.registerType( Point.class, NTPoint );

        // Below components are not public API, but are made available for internal
        // globalProcedures to call, and to provide temporary workarounds for the following
        // patterns:
        //  - Batch-transaction imports (GDAPI, needs to be real and passed to background processing threads)
        //  - Group-transaction writes (same pattern as above, but rather than splitting large transactions,
        //                              combine lots of small ones)
        //  - Bleeding-edge performance (KernelTransaction, to bypass overhead of working with Core API)
        globalProcedures.registerComponent( DependencyResolver.class, ctx -> ctx.get( DEPENDENCY_RESOLVER ), false );
        globalProcedures.registerComponent( KernelTransaction.class, ctx -> ctx.get( KERNEL_TRANSACTION ), false );
        globalProcedures.registerComponent( GraphDatabaseAPI.class, ctx -> ctx.get( DATABASE_API ), false );

        // Register injected public API components
        globalProcedures.registerComponent( Log.class, ctx -> proceduresLog, true );
        globalProcedures.registerComponent( ProcedureTransaction.class, new ProcedureTransactionProvider(), true );
        globalProcedures.registerComponent( org.neo4j.procedure.TerminationGuard.class, new TerminationGuardProvider(), true );
        globalProcedures.registerComponent( SecurityContext.class, ctx -> ctx.get( SECURITY_CONTEXT ), true );
        globalProcedures.registerComponent( FulltextAdapter.class, ctx -> ctx.get( DEPENDENCY_RESOLVER ).resolveDependency( FulltextAdapter.class ), true );
        globalProcedures.registerComponent( DataCollector.class, ctx -> ctx.get( DEPENDENCY_RESOLVER ).resolveDependency( DataCollector.class ), false );

        // Edition procedures
        try
        {
            editionModule.registerProcedures( globalProcedures, procedureConfig );
        }
        catch ( KernelException e )
        {
            internalLog.error( "Failed to register built-in edition procedures at start up: " + e.getMessage() );
        }

        platform.life.add( globalProcedures );
        platform.dependencies.satisfyDependency( globalProcedures );
        return globalProcedures;
    }

    private static BoltServer createBoltServer( PlatformModule platform, AbstractEditionModule edition, DatabaseManager databaseManager )
    {
        return new BoltServer( databaseManager, platform.jobScheduler,
                platform.connectorPortRegister, edition.getConnectionTracker(), platform.usageData, platform.config, platform.clock, platform.monitors,
                platform.logService, platform.dependencies );
    }

    private static DatabaseManager createAndInitializeDatabaseManager( PlatformModule platform, AbstractEditionModule edition,
            GraphDatabaseFacade facade, GlobalProcedures globalProcedures, Logger logger )
    {
        DatabaseManager databaseManager = edition.createDatabaseManager( facade, platform, edition, globalProcedures, logger );
        if ( !edition.handlesDatabaseManagerLifecycle() )
        {
            // only add database manager to the lifecycle when edition doesn't manage it already
            platform.life.add( databaseManager );
        }
        platform.dependencies.satisfyDependency( databaseManager );
        return databaseManager;
    }
}
