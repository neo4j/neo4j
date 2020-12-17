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
package org.neo4j.graphdb.facade;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.bolt.BoltServer;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManagementServiceImpl;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.DefaultDatabaseInitializer;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.UnableToStartDatabaseException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.collector.DataCollector;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.builtin.SpecialBuiltInProcedures;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.procedure.impl.ProcedureLoginContextTransformer;
import org.neo4j.procedure.impl.ProcedureTransactionProvider;
import org.neo4j.procedure.impl.TerminationGuardProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.DisabledNeoWebServer;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.factory.module.edition.CommunityEditionModule.tryResolveOrCreate;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * This is the main factory for creating database instances. It delegates creation to two different modules
 * ({@link GlobalModule} and {@link AbstractEditionModule}),
 * which create all the specific services needed to run a graph database.
 * <p>
 * To create test versions of databases, override an edition factory (e.g. {@link org.neo4j.kernel.impl.factory
 * .CommunityFacadeFactory}), and replace modules
 * with custom versions that instantiate alternative services.
 */
public class DatabaseManagementServiceFactory
{
    protected final DbmsInfo dbmsInfo;
    private final Function<GlobalModule,AbstractEditionModule> editionFactory;

    public DatabaseManagementServiceFactory( DbmsInfo dbmsInfo, Function<GlobalModule,AbstractEditionModule> editionFactory )
    {
        this.dbmsInfo = dbmsInfo;
        this.editionFactory = editionFactory;
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of {@link org
     * .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param config configuration
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public DatabaseManagementService build( Config config, final ExternalDependencies dependencies )
    {
        GlobalModule globalModule = createGlobalModule( config, dependencies );
        AbstractEditionModule edition = editionFactory.apply( globalModule );
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        LifeSupport globalLife = globalModule.getGlobalLife();

        LogService logService = globalModule.getLogService();
        Log internalLog = logService.getInternalLog( getClass() );
        DatabaseManager<?> databaseManager = edition.createDatabaseManager( globalModule );
        DatabaseManagementService managementService = createManagementService( globalModule, globalLife, internalLog, databaseManager );
        globalDependencies.satisfyDependencies( managementService );

        edition.bootstrapFabricServices();

        setupProcedures( globalModule, edition, databaseManager );

        edition.registerSystemGraphComponents( globalModule.getSystemGraphComponents(), globalModule );
        globalLife.add( edition.createSystemGraphInitializer( globalModule ) );

        var dbmsRuntimeSystemGraphComponent = new DbmsRuntimeSystemGraphComponent( globalModule.getGlobalConfig() );
        globalModule.getSystemGraphComponents().register( dbmsRuntimeSystemGraphComponent );

        edition.createDefaultDatabaseResolver( globalModule );
        globalDependencies.satisfyDependency( edition.getDefaultDatabaseResolver() );

        edition.createSecurityModule( globalModule );
        SecurityProvider securityProvider = edition.getSecurityProvider();
        globalDependencies.satisfyDependencies( securityProvider.authManager() );

        var dbmsRuntimeRepository =
                edition.createAndRegisterDbmsRuntimeRepository( globalModule, databaseManager, globalDependencies, dbmsRuntimeSystemGraphComponent );
        globalDependencies.satisfyDependency( dbmsRuntimeRepository );

        globalLife.add( new DefaultDatabaseInitializer( databaseManager ) );

        globalLife.add( globalModule.getGlobalExtensions() );
        BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI = edition.createBoltDatabaseManagementServiceProvider( globalDependencies,
                managementService, globalModule.getGlobalMonitors(), globalModule.getGlobalClock(), logService );
        globalLife.add( createBoltServer( globalModule, edition, boltGraphDatabaseManagementServiceSPI, databaseManager.databaseIdRepository() ) );
        var webServer = createWebServer( edition, managementService, globalDependencies, config, globalModule.getLogService().getUserLogProvider() );
        globalDependencies.satisfyDependency( webServer );
        globalLife.add( webServer );

        startDatabaseServer( globalModule, globalLife, internalLog, databaseManager, managementService );

        return managementService;
    }

    protected DatabaseManagementService createManagementService( GlobalModule globalModule, LifeSupport globalLife, Log internalLog,
            DatabaseManager<?> databaseManager )
    {
        return new DatabaseManagementServiceImpl( databaseManager, globalModule.getGlobalAvailabilityGuard(),
                globalLife, globalModule.getDatabaseEventListeners(), globalModule.getTransactionEventListeners(), internalLog );
    }

    private Lifecycle createWebServer( AbstractEditionModule edition, DatabaseManagementService managementService,
                                       Dependencies globalDependencies, Config config, LogProvider userLogProvider )
    {
        if ( shouldEnableWebServer( config ) )
        {
            return edition.createWebServer( managementService, globalDependencies, config, userLogProvider, dbmsInfo );
        }
        return new DisabledNeoWebServer();
    }

    private boolean shouldEnableWebServer( Config config )
    {
        return (config.get( HttpConnector.enabled ) || config.get( HttpsConnector.enabled )) && !config.get( ServerSettings.http_enabled_modules ).isEmpty();
    }

    private static void startDatabaseServer( GlobalModule globalModule, LifeSupport globalLife, Log internalLog, DatabaseManager<?> databaseManager,
            DatabaseManagementService managementService )
    {

        RuntimeException startupException = null;
        try
        {
            databaseManager.initialiseSystemDatabase();
            globalLife.start();

            DatabaseStateService databaseStateService = globalModule.getGlobalDependencies().resolveDependency( DatabaseStateService.class );

            verifySystemDatabaseStart( databaseManager, databaseStateService );
        }
        catch ( Throwable throwable )
        {
            String message = "Error starting Neo4j database server at " + globalModule.getNeo4jLayout().databasesDirectory();
            startupException = new RuntimeException( message, throwable );
            internalLog.error( message, throwable );
        }
        finally
        {
            if ( startupException != null )
            {
                try
                {
                    managementService.shutdown();
                }
                catch ( Throwable shutdownError )
                {
                    startupException.addSuppressed( shutdownError );
                }
            }
        }

        if ( startupException != null )
        {
            internalLog.error( "Failed to start database server.", startupException );
            throw startupException;
        }
    }

    private static void verifySystemDatabaseStart( DatabaseManager<?> databaseManager, DatabaseStateService dbStateService )
    {
        Optional<? extends DatabaseContext> databaseContext = databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID );
        if ( databaseContext.isEmpty() )
        {
            throw new UnableToStartDatabaseException( SYSTEM_DATABASE_NAME + " not found." );
        }

        Optional<Throwable> failure = dbStateService.causeOfFailure( NAMED_SYSTEM_DATABASE_ID );
        if ( failure.isPresent() )
        {
            throw new UnableToStartDatabaseException( SYSTEM_DATABASE_NAME + " failed to start.", failure.get() );
        }
    }

    /**
     * Create the platform module. Override to replace with custom module.
     */
    protected GlobalModule createGlobalModule( Config config, final ExternalDependencies dependencies )
    {
        return new GlobalModule( config, dbmsInfo, dependencies );
    }

    /**
     * Creates and registers the systems procedures, including those which belong to a particular edition.
     * N.B. This method takes a {@link DatabaseManager} as an unused parameter *intentionally*, in
     * order to enforce that the databaseManager must be constructed first.
     */
    @SuppressWarnings( "unused" )
    private static void setupProcedures( GlobalModule globalModule, AbstractEditionModule editionModule, DatabaseManager<?> databaseManager )
    {
        Supplier<GlobalProcedures> procedureInitializer = () ->
        {
            Config globalConfig = globalModule.getGlobalConfig();
            Path proceduresDirectory = globalConfig.get( GraphDatabaseSettings.plugin_dir );
            LogService logService = globalModule.getLogService();
            Log internalLog = logService.getInternalLog( GlobalProcedures.class );
            Log proceduresLog = logService.getUserLog( GlobalProcedures.class );

            ProcedureConfig procedureConfig = new ProcedureConfig( globalConfig );
            Edition neo4jEdition = globalModule.getDbmsInfo().edition;
            SpecialBuiltInProcedures builtInProcedures = new SpecialBuiltInProcedures( Version.getNeo4jVersion(), neo4jEdition.toString() );
            GlobalProceduresRegistry globalProcedures = new GlobalProceduresRegistry( builtInProcedures, proceduresDirectory, internalLog, procedureConfig );

            globalProcedures.registerType( Node.class, NTNode );
            globalProcedures.registerType( NodeValue.class, NTNode );
            globalProcedures.registerType( Relationship.class, NTRelationship );
            globalProcedures.registerType( RelationshipValue.class, NTRelationship );
            globalProcedures.registerType( org.neo4j.graphdb.Path.class, NTPath );
            globalProcedures.registerType( PathValue.class, NTPath );
            globalProcedures.registerType( Geometry.class, NTGeometry );
            globalProcedures.registerType( Point.class, NTPoint );
            globalProcedures.registerType( PointValue.class, NTPoint );

            // Below components are not public API, but are made available for internal
            // procedures to call, and to provide temporary workarounds for the following
            // patterns:
            //  - Batch-transaction imports (GDAPI, needs to be real and passed to background processing threads)
            //  - Group-transaction writes (same pattern as above, but rather than splitting large transactions,
            //                              combine lots of small ones)
            //  - Bleeding-edge performance (KernelTransaction, to bypass overhead of working with Core API)
            globalProcedures.registerComponent( DependencyResolver.class, Context::dependencyResolver, false );
            globalProcedures.registerComponent( KernelTransaction.class, ctx -> ctx.internalTransaction().kernelTransaction(), false );
            globalProcedures.registerComponent( GraphDatabaseAPI.class, Context::graphDatabaseAPI, false );
            globalProcedures.registerComponent( SystemGraphComponents.class, ctx -> globalModule.getSystemGraphComponents(), false );
            globalProcedures.registerComponent( ValueMapper.class, Context::valueMapper, true );

            // Register injected public API components
            globalProcedures.registerComponent( Log.class, ctx -> proceduresLog, true );
            globalProcedures.registerComponent( Transaction.class, new ProcedureTransactionProvider(), true );
            globalProcedures.registerComponent( org.neo4j.procedure.TerminationGuard.class, new TerminationGuardProvider(), true );
            globalProcedures.registerComponent( SecurityContext.class, Context::securityContext, true );
            globalProcedures.registerComponent( ProcedureCallContext.class, Context::procedureCallContext, true );
            globalProcedures.registerComponent( FulltextAdapter.class, ctx -> ctx.dependencyResolver().resolveDependency( FulltextAdapter.class ), true );
            globalProcedures.registerComponent( GraphDatabaseService.class,
                    ctx -> new GraphDatabaseFacade( (GraphDatabaseFacade) ctx.graphDatabaseAPI(), new ProcedureLoginContextTransformer( ctx ) ), true );

            globalProcedures.registerComponent( DataCollector.class, ctx -> ctx.dependencyResolver().resolveDependency( DataCollector.class ), false );

            // Edition procedures
            try
            {
                editionModule.registerProcedures( globalProcedures, procedureConfig, globalModule, databaseManager );
            }
            catch ( KernelException e )
            {
                internalLog.error( "Failed to register built-in edition procedures at start up: " + e.getMessage() );
            }
            globalModule.getGlobalLife().add( globalProcedures );

            return globalProcedures;
        };
        GlobalProcedures procedures = tryResolveOrCreate( GlobalProcedures.class, globalModule.getExternalDependencyResolver(), procedureInitializer );
        if ( procedures instanceof Consumer )
        {
            ((Consumer) procedures).accept( procedureInitializer );
        }
        globalModule.getGlobalDependencies().satisfyDependency( procedures );
    }

    private static BoltServer createBoltServer( GlobalModule globalModule, AbstractEditionModule edition,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI, DatabaseIdRepository databaseIdRepository )
    {
        return new BoltServer( boltGraphDatabaseManagementServiceSPI, globalModule.getJobScheduler(), globalModule.getConnectorPortRegister(),
                               edition.getConnectionTracker(), databaseIdRepository, globalModule.getGlobalConfig(), globalModule.getGlobalClock(),
                               globalModule.getGlobalMonitors(), globalModule.getLogService(), globalModule.getGlobalDependencies(),
                               edition.getBoltAuthManager( globalModule.getGlobalDependencies() ), edition.getBoltInClusterAuthManager(),
                               globalModule.getMemoryPools(), edition.getDefaultDatabaseResolver() );
    }
}
