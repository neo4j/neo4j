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
package org.neo4j.graphdb.factory.module.edition;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.cypher.internal.javacompat.CommunityCypherEngineProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.dbms.database.DefaultDatabaseManager;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.dbms.procedures.StandaloneDatabaseStateProcedure;
import org.neo4j.exceptions.KernelException;
import org.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.GlobalAvailabilityGuardController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;
import org.neo4j.procedure.builtin.routing.SingleInstanceRoutingProcedureInstaller;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockManager;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_FILENAME_PREDICATE;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends StandaloneEditionModule
{
    protected final SslPolicyLoader sslPolicyLoader;
    protected final GlobalModule globalModule;
    private final CompositeDatabaseAvailabilityGuard globalAvailabilityGuard;
    private final FabricServicesBootstrap fabricServicesBootstrap;

    public CommunityEditionModule( GlobalModule globalModule )
    {
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        SystemNanoClock globalClock = globalModule.getGlobalClock();
        DependencyResolver externalDependencies = globalModule.getExternalDependencyResolver();
        this.globalModule = globalModule;

        watcherServiceFactory = databaseLayout -> createDatabaseFileSystemWatcher( globalModule.getFileWatcher(), databaseLayout,
                logService, fileWatcherFileNameFilter() );

        this.sslPolicyLoader = SslPolicyLoader.create( globalConfig, logService.getInternalLogProvider() );
        globalDependencies.satisfyDependency( sslPolicyLoader ); // for bolt and web server
        globalDependencies.satisfyDependency( new DatabaseOperationCounts.Counter() ); // for global metrics

        LocksFactory lockFactory = createLockFactory( globalConfig, logService );
        locksSupplier = () -> createLockManager( lockFactory, globalConfig, globalClock );
        statementLocksFactoryProvider = locks -> createStatementLocksFactory( locks, globalConfig, logService );

        idContextFactory = tryResolveOrCreate( IdContextFactory.class, externalDependencies, () -> createIdContextFactory( globalModule ) );

        tokenHoldersProvider = createTokenHolderProvider( globalModule );

        commitProcessFactory = new CommunityCommitProcessFactory();

        constraintSemantics = createSchemaRuleVerifier();

        ioLimiter = IOLimiter.UNLIMITED;

        connectionTracker = globalDependencies.satisfyDependency( createConnectionTracker() );
        globalAvailabilityGuard = globalModule.getGlobalAvailabilityGuard();

        fabricServicesBootstrap = new FabricServicesBootstrap.Community( globalModule.getGlobalLife(), globalDependencies, globalModule.getLogService() );
    }

    protected Function<NamedDatabaseId,TokenHolders> createTokenHolderProvider( GlobalModule platform )
    {
        Config globalConfig = platform.getGlobalConfig();
        return databaseId -> {
            DatabaseManager<?> databaseManager = platform.getGlobalDependencies().resolveDependency( DefaultDatabaseManager.class );
            Supplier<Kernel> kernelSupplier = () ->
            {
                DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseId )
                        .orElseThrow( () -> new IllegalStateException( "Default and system database kernels should always be accessible" ) );
                return databaseContext.dependencies().resolveDependency( Kernel.class );
            };
            return new TokenHolders(
                    new DelegatingTokenHolder( createPropertyKeyCreator( globalConfig, databaseId, kernelSupplier ), TYPE_PROPERTY_KEY ),
                    new DelegatingTokenHolder( createLabelIdCreator( globalConfig, databaseId, kernelSupplier ), TYPE_LABEL ),
                    new DelegatingTokenHolder( createRelationshipTypeCreator( globalConfig, databaseId, kernelSupplier ), TYPE_RELATIONSHIP_TYPE ) );
        };
    }

    private static IdContextFactory createIdContextFactory( GlobalModule globalModule )
    {
        return IdContextFactoryBuilder.of( globalModule.getFileSystem(), globalModule.getJobScheduler(), globalModule.getGlobalConfig(),
                globalModule.getTracers().getPageCacheTracer() ).build();
    }

    protected Predicate<String> fileWatcherFileNameFilter()
    {
        return communityFileWatcherFileNameFilter();
    }

    static Predicate<String> communityFileWatcherFileNameFilter()
    {
        return DEFAULT_FILENAME_PREDICATE;
    }

    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new StandardConstraintSemantics();
    }

    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new SimpleStatementLocksFactory( locks );
    }

    protected static TokenCreator createRelationshipTypeCreator( Config config, NamedDatabaseId namedDatabaseId, Supplier<Kernel> kernelSupplier )
    {
        return createReadOnlyTokens( config, namedDatabaseId ) ? new ReadOnlyTokenCreator() : new DefaultRelationshipTypeCreator( kernelSupplier );
    }

    protected static TokenCreator createPropertyKeyCreator( Config config, NamedDatabaseId namedDatabaseId, Supplier<Kernel> kernelSupplier )
    {
        return createReadOnlyTokens( config, namedDatabaseId ) ? new ReadOnlyTokenCreator() : new DefaultPropertyTokenCreator( kernelSupplier );
    }

    protected static TokenCreator createLabelIdCreator( Config config, NamedDatabaseId namedDatabaseId, Supplier<Kernel> kernelSupplier )
    {
        return createReadOnlyTokens( config, namedDatabaseId ) ? new ReadOnlyTokenCreator() : new DefaultLabelIdCreator( kernelSupplier );
    }

    @Override
    public QueryEngineProvider getQueryEngineProvider()
    {
        return new CommunityCypherEngineProvider();
    }

    @Override
    public DatabaseStartupController getDatabaseStartupController()
    {
        return new GlobalAvailabilityGuardController( globalAvailabilityGuard );
    }

    @Override
    public Lifecycle createWebServer( DatabaseManagementService managementService, Dependencies globalDependencies, Config config,
            LogProvider userLogProvider, DbmsInfo dbmsInfo )
    {
        return new CommunityNeoWebServer( managementService, globalDependencies, config, userLogProvider, dbmsInfo );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager ) throws KernelException
    {
        globalProcedures.register( new StandaloneDatabaseStateProcedure( databaseStateService,
                databaseManager.databaseIdRepository(), globalModule.getGlobalConfig().get( BoltConnector.advertised_address ).toString() ) );
    }

    @Override
    protected BaseRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        return new SingleInstanceRoutingProcedureInstaller( databaseManager, portRegister, config, logProvider );
    }

    @Override
    public SystemGraphInitializer createSystemGraphInitializer( GlobalModule globalModule )
    {
        DependencyResolver globalDependencies = globalModule.getGlobalDependencies();
        Supplier<GraphDatabaseService> systemSupplier = systemSupplier( globalDependencies );
        var systemGraphComponents = globalModule.getSystemGraphComponents();
        SystemGraphInitializer initializer =
                CommunityEditionModule.tryResolveOrCreate( SystemGraphInitializer.class, globalModule.getExternalDependencyResolver(),
                        () -> new DefaultSystemGraphInitializer( systemSupplier, systemGraphComponents ) );
        return globalModule.getGlobalDependencies().satisfyDependency( initializer );
    }

    @Override
    public void registerSystemGraphComponents( SystemGraphComponents systemGraphComponents, GlobalModule globalModule )
    {
        var systemGraphComponent = new DefaultSystemGraphComponent( globalModule.getGlobalConfig() );
        systemGraphComponents.register( systemGraphComponent );
    }

    protected Supplier<GraphDatabaseService> systemSupplier( DependencyResolver dependencies )
    {
        return () ->
        {
            DatabaseManager<?> databaseManager = dependencies.resolveDependency( DatabaseManager.class );
            return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).orElseThrow(
                    () -> new RuntimeException( "No database called `" + SYSTEM_DATABASE_NAME + "` was found." ) ).databaseFacade();
        };
    }

    private void setupSecurityGraphInitializer( GlobalModule globalModule )
    {
        Config config = globalModule.getGlobalConfig();
        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        LogProvider logProvider = globalModule.getLogService().getUserLogProvider();
        Log securityLog = logProvider.getLog( UserSecurityGraphComponent.class );

        var communityComponent = CommunitySecurityModule.createSecurityComponent( securityLog, config, fileSystem, logProvider );

        Dependencies dependencies = globalModule.getGlobalDependencies();
        SystemGraphComponents systemGraphComponents = dependencies.resolveDependency( SystemGraphComponents.class );
        systemGraphComponents.register( communityComponent );
   }

    @Override
    public void createSecurityModule( GlobalModule globalModule )
    {
        setSecurityProvider( makeSecurityModule( globalModule ) );
    }

    private SecurityProvider makeSecurityModule( GlobalModule globalModule )
    {
        setupSecurityGraphInitializer( globalModule );
        if ( globalModule.getGlobalConfig().get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule =
                    new CommunitySecurityModule( globalModule.getLogService(), globalModule.getGlobalConfig(), globalModule.getGlobalDependencies() );
            securityModule.setup();
            return securityModule;
        }
        return NoAuthSecurityProvider.INSTANCE;
    }

    @Override
    public void createDefaultDatabaseResolver( GlobalModule globalModule )
    {
        Supplier<GraphDatabaseService> systemDbSupplier = systemSupplier( globalModule.getGlobalDependencies() );
        CommunityDefaultDatabaseResolver defaultDatabaseResolver = new CommunityDefaultDatabaseResolver( globalModule.getGlobalConfig(), systemDbSupplier );
        globalModule.getTransactionEventListeners().registerTransactionEventListener( SYSTEM_DATABASE_NAME, defaultDatabaseResolver );
        setDefaultDatabaseResolver( defaultDatabaseResolver );
    }

    public static <T> T tryResolveOrCreate( Class<T> clazz, DependencyResolver dependencies, Supplier<T> newInstanceMethod )
    {
        return dependencies.containsDependency( clazz ) ? dependencies.resolveDependency( clazz ) : newInstanceMethod.get();
    }

    private static boolean createReadOnlyTokens( Config config, NamedDatabaseId namedDatabaseId )
    {
        return !namedDatabaseId.isSystemDatabase() && config.get( GraphDatabaseSettings.read_only );
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var kernelDatabaseManagementService = createBoltKernelDatabaseManagementServiceProvider(dependencies, managementService, monitors, clock, logService);
        return fabricServicesBootstrap.createBoltDatabaseManagementServiceProvider( kernelDatabaseManagementService, managementService, monitors, clock );
    }

    protected BoltGraphDatabaseManagementServiceSPI createBoltKernelDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var config = dependencies.resolveDependency( Config.class );
        var bookmarkAwaitDuration =  config.get( GraphDatabaseSettings.bookmark_ready_timeout );
        return new BoltKernelDatabaseManagementServiceProvider( managementService, monitors, clock, bookmarkAwaitDuration );
    }

    @Override
    public void bootstrapFabricServices()
    {
        fabricServicesBootstrap.bootstrapServices();
    }
}
