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
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.bolt.BoltServer;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManagementServiceImpl;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DefaultDatabaseInitializer;
import org.neo4j.dbms.database.UnableToStartDatabaseException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.config.Setting;
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
import org.neo4j.kernel.impl.api.dbms.NonTransactionalDbmsOperations;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.ProcedureTransaction;
import org.neo4j.procedure.builtin.SpecialBuiltInProcedures;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.procedure.impl.ProcedureLoginContextTransformer;
import org.neo4j.procedure.impl.ProcedureTransactionProvider;
import org.neo4j.procedure.impl.TerminationGuardProvider;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

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
    protected final DatabaseInfo databaseInfo;
    private final Function<GlobalModule,AbstractEditionModule> editionFactory;

    public DatabaseManagementServiceFactory( DatabaseInfo databaseInfo, Function<GlobalModule,AbstractEditionModule> editionFactory )
    {
        this.databaseInfo = databaseInfo;
        this.editionFactory = editionFactory;
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of {@link org
     * .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param params configuration parameters
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public DatabaseManagementService build( File storeDir, Map<Setting<?>,Object> params, final ExternalDependencies dependencies )
    {
        return build( storeDir, Config.defaults( params ), dependencies );
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of {@link org
     * .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param storeDir the directory where the Neo4j data store is located
     * @param config configuration
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public DatabaseManagementService build( File storeDir, Config config, final ExternalDependencies dependencies )
    {
        GlobalModule globalModule = createGlobalModule( storeDir, config, dependencies );
        AbstractEditionModule edition = editionFactory.apply( globalModule );
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        LifeSupport globalLife = globalModule.getGlobalLife();

        LogService logService = globalModule.getLogService();
        Log internalLog = logService.getInternalLog( getClass() );
        DatabaseManager<?> databaseManager = edition.createDatabaseManager( globalModule );
        DatabaseManagementService managementService = new DatabaseManagementServiceImpl( databaseManager, globalModule.getGlobalAvailabilityGuard(),
                globalLife, globalModule.getDatabaseEventListeners(), globalModule.getTransactionEventListeners(), internalLog );
        globalDependencies.satisfyDependencies( managementService );

        GlobalProcedures globalProcedures = setupProcedures( globalModule, edition, databaseManager );
        globalDependencies.satisfyDependency( new NonTransactionalDbmsOperations( globalProcedures ) );

        edition.createSystemGraphInitializer( globalModule, databaseManager );
        edition.createSecurityModule( globalModule );
        SecurityProvider securityProvider = edition.getSecurityProvider();
        globalDependencies.satisfyDependencies( securityProvider.authManager() );
        globalDependencies.satisfyDependencies( securityProvider.userManagerSupplier() );

        globalLife.add( new DefaultDatabaseInitializer( databaseManager ) );

        globalLife.add( globalModule.getGlobalExtensions() );
        BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI = edition.createBoltDatabaseManagementServiceProvider( globalDependencies,
                managementService, globalModule.getGlobalMonitors(), globalModule.getGlobalClock(), logService );
        globalLife.add( createBoltServer( globalModule, edition, boltGraphDatabaseManagementServiceSPI, databaseManager.databaseIdRepository() ) );
        globalDependencies.satisfyDependency( edition.globalTransactionCounter() );

        startDatabaseServer( globalModule, globalLife, internalLog, databaseManager, managementService );

        return managementService;
    }

    private static void startDatabaseServer( GlobalModule globalModule, LifeSupport globalLife, Log internalLog, DatabaseManager<?> databaseManager,
            DatabaseManagementService managementService )
    {

        RuntimeException startupException = null;
        try
        {
            databaseManager.initialiseSystemDatabase();
            globalLife.start();

            verifySystemDatabaseStart( databaseManager );
        }
        catch ( Throwable throwable )
        {
            String message = "Error starting database server at " + globalModule.getStoreLayout().storeDirectory();
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

    private static void verifySystemDatabaseStart( DatabaseManager<?> databaseManager )
    {
        Optional<? extends DatabaseContext> databaseContext = databaseManager.getDatabaseContext( SYSTEM_DATABASE_ID );
        if ( databaseContext.isEmpty() )
        {
            throw new UnableToStartDatabaseException( SYSTEM_DATABASE_NAME + " not found." );
        }

        DatabaseContext systemContext = databaseContext.get();
        if ( databaseContext.get().isFailed() )
        {
            throw new UnableToStartDatabaseException( SYSTEM_DATABASE_NAME + " failed to start.", systemContext.failureCause() );
        }
    }

    /**
     * Create the platform module. Override to replace with custom module.
     */
    protected GlobalModule createGlobalModule( File storeDir, Config config, final ExternalDependencies dependencies )
    {
        return new GlobalModule( storeDir, config, databaseInfo, dependencies );
    }

    /**
     * Creates and registers the systems procedures, including those which belong to a particular edition.
     * N.B. This method takes a {@link DatabaseManager} as an unused parameter *intentionally*, in
     * order to enforce that the databaseManager must be constructed first.
     */
    @SuppressWarnings( "unused" )
    private static GlobalProcedures setupProcedures( GlobalModule globalModule, AbstractEditionModule editionModule, DatabaseManager<?> databaseManager )
    {
        Config globalConfig = globalModule.getGlobalConfig();
        File proceduresDirectory = globalConfig.get( GraphDatabaseSettings.plugin_dir ).toFile();
        LogService logService = globalModule.getLogService();
        Log internalLog = logService.getInternalLog( GlobalProcedures.class );
        Log proceduresLog = logService.getUserLog( GlobalProcedures.class );

        ProcedureConfig procedureConfig = new ProcedureConfig( globalConfig );
        Edition neo4jEdition = globalModule.getDatabaseInfo().edition;
        SpecialBuiltInProcedures builtInProcedures = new SpecialBuiltInProcedures( Version.getNeo4jVersion(), neo4jEdition.toString() );
        GlobalProceduresRegistry globalProcedures = new GlobalProceduresRegistry( builtInProcedures, proceduresDirectory, internalLog, procedureConfig );

        globalProcedures.registerType( Node.class, NTNode );
        globalProcedures.registerType( NodeValue.class, NTNode );
        globalProcedures.registerType( Relationship.class, NTRelationship );
        globalProcedures.registerType( RelationshipValue.class, NTRelationship );
        globalProcedures.registerType( Path.class, NTPath );
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
        globalProcedures.registerComponent( KernelTransaction.class, Context::kernelTransaction, false );
        globalProcedures.registerComponent( GraphDatabaseAPI.class, Context::graphDatabaseAPI, false );

        // Register injected public API components
        globalProcedures.registerComponent( Log.class, ctx -> proceduresLog, true );
        globalProcedures.registerComponent( ProcedureTransaction.class, new ProcedureTransactionProvider(), true );
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
        globalModule.getGlobalDependencies().satisfyDependency( globalProcedures );
        return globalProcedures;
    }

    private static BoltServer createBoltServer( GlobalModule platform, AbstractEditionModule edition,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI, DatabaseIdRepository databaseIdRepository )
    {
        return new BoltServer( boltGraphDatabaseManagementServiceSPI, platform.getJobScheduler(), platform.getConnectorPortRegister(),
                edition.getConnectionTracker(), databaseIdRepository, platform.getGlobalConfig(), platform.getGlobalClock(),
                platform.getGlobalMonitors(), platform.getLogService(), platform.getGlobalDependencies(),
                edition.getBoltAuthManager( platform.getGlobalDependencies() ) );
    }
}
