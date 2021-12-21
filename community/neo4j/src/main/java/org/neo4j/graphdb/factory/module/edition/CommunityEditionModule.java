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
package org.neo4j.graphdb.factory.module.edition;

import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.CommunityDatabaseStateService;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseIdCacheClearingListener;
import org.neo4j.dbms.database.DatabaseInfoService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.dbms.database.DefaultDatabaseContextFactory;
import org.neo4j.dbms.database.DefaultDatabaseContextFactoryComponents;
import org.neo4j.dbms.database.DefaultDatabaseManager;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.dbms.database.StandaloneDatabaseInfoService;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.identity.DefaultIdentityModule;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphComponent;
import org.neo4j.exceptions.KernelException;
import org.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.AbstractRoutingProcedureInstaller;
import org.neo4j.procedure.builtin.routing.ClientRoutingDomainChecker;
import org.neo4j.procedure.builtin.routing.SingleInstanceRoutingProcedureInstaller;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.rest.repr.CommunityAuthConfigProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends StandaloneEditionModule implements DefaultDatabaseContextFactoryComponents
{
    protected final SslPolicyLoader sslPolicyLoader;
    protected final GlobalModule globalModule;
    protected final ServerIdentity identityModule;
    private final FabricServicesBootstrap fabricServicesBootstrap;

    protected DatabaseStateService databaseStateService;
    protected ReadOnlyDatabases globalReadOnlyChecker;

    public CommunityEditionModule( GlobalModule globalModule )
    {
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        this.globalModule = globalModule;

        this.sslPolicyLoader = SslPolicyLoader.create( globalConfig, logService.getInternalLogProvider() );
        globalDependencies.satisfyDependency( sslPolicyLoader ); // for bolt and web server
        globalDependencies.satisfyDependency( new DatabaseOperationCounts.Counter() ); // for global metrics

        globalDependencies.satisfyDependency( createAuthConfigProvider( globalModule ) );

        identityModule = DefaultIdentityModule.fromGlobalModule( globalModule );
        globalDependencies.satisfyDependency( identityModule );

        connectionTracker = globalDependencies.satisfyDependency( createConnectionTracker() );

        fabricServicesBootstrap = new FabricServicesBootstrap.Community( globalModule.getGlobalLife(), globalDependencies, globalModule.getLogService() );
    }

    @Override
    public DatabaseManager<? extends DatabaseContext> createDatabaseManager( GlobalModule globalModule )
    {
        var databaseContextFactory = new DefaultDatabaseContextFactory( globalModule,
                                                                        getTransactionMonitorFactory(),
                                                                        createIdContextFactory( globalModule ),
                                                                        createCommitProcessFactory(),
                                                                        this );
        var databaseManager = new DefaultDatabaseManager( globalModule, databaseContextFactory );
        databaseStateService = new CommunityDatabaseStateService( databaseManager );

        globalModule.getGlobalLife().add( databaseManager );
        globalModule.getGlobalDependencies().satisfyDependency( databaseManager );
        globalModule.getGlobalDependencies().satisfyDependency( databaseStateService );

        globalReadOnlyChecker = createGlobalReadOnlyChecker( databaseManager, globalModule.getGlobalConfig(),
                                                             globalModule.getTransactionEventListeners(), globalModule.getGlobalLife(),
                                                             globalModule.getLogService().getInternalLogProvider() );

        var databaseIdCacheCleaner = new DatabaseIdCacheClearingListener( databaseManager.databaseIdRepository() );
        globalModule.getTransactionEventListeners().registerTransactionEventListener( SYSTEM_DATABASE_NAME, databaseIdCacheCleaner );

        return databaseManager;
    }

    @Override
    public Lifecycle createWebServer( DatabaseManagementService managementService, Dependencies globalDependencies, Config config,
                                      LogProvider userLogProvider, DbmsInfo dbmsInfo )
    {
        return new CommunityNeoWebServer( managementService, globalDependencies, config, userLogProvider, dbmsInfo,
                                          globalModule.getMemoryPools(), globalModule.getTransactionManager(), globalModule.getGlobalClock() );
    }

    @Override
    public DatabaseInfoService createDatabaseInfoService( DatabaseManager<?> databaseManager )
    {
        var address = globalModule.getGlobalConfig().get( BoltConnector.advertised_address );
        return new StandaloneDatabaseInfoService( identityModule.serverId(), address, databaseManager , databaseStateService, globalReadOnlyChecker );
    }

    @Override
    public void registerEditionSpecificProcedures( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager ) throws KernelException
    {
    }

    @Override
    protected AbstractRoutingProcedureInstaller createRoutingProcedureInstaller( GlobalModule globalModule, DatabaseManager<?> databaseManager,
                                                                                 ClientRoutingDomainChecker clientRoutingDomainChecker )
    {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        LogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        return new SingleInstanceRoutingProcedureInstaller( databaseManager, clientRoutingDomainChecker, portRegister, config, logProvider );
    }

    @Override
    protected AuthConfigProvider createAuthConfigProvider( GlobalModule globalModule )
    {
        return new CommunityAuthConfigProvider();
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
        var config = globalModule.getGlobalConfig();
        var log = globalModule.getLogService().getInternalLogProvider();
        var clock = globalModule.getGlobalClock();
        var systemGraphComponent = new DefaultSystemGraphComponent( config, clock );
        systemGraphComponents.register( systemGraphComponent );
        var communityTopologyGraphComponentComponent = new CommunityTopologyGraphComponent( config, log );
        systemGraphComponents.register( communityTopologyGraphComponentComponent );
    }

    protected static Supplier<GraphDatabaseService> systemSupplier( DependencyResolver dependencies )
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
        AbstractSecurityLog securityLog = new CommunitySecurityLog( logProvider.getLog( UserSecurityGraphComponent.class ) );

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
        globalModule.getGlobalDependencies().satisfyDependency( CommunitySecurityLog.NULL_LOG );
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

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock, LogService logService )
    {
        var kernelDatabaseManagementService = createBoltKernelDatabaseManagementServiceProvider( dependencies, managementService, monitors, clock );
        return fabricServicesBootstrap.createBoltDatabaseManagementServiceProvider( kernelDatabaseManagementService, managementService, monitors, clock );
    }

    protected static BoltGraphDatabaseManagementServiceSPI createBoltKernelDatabaseManagementServiceProvider( Dependencies dependencies,
            DatabaseManagementService managementService, Monitors monitors, SystemNanoClock clock )
    {
        var config = dependencies.resolveDependency( Config.class );
        var bookmarkAwaitDuration =  config.get( GraphDatabaseSettings.bookmark_ready_timeout );
        return new BoltKernelDatabaseManagementServiceProvider( managementService, monitors, clock, bookmarkAwaitDuration );
    }

    protected CommitProcessFactory createCommitProcessFactory()
    {
        return new CommunityCommitProcessFactory();
    }

    @Override
    public void bootstrapFabricServices()
    {
        fabricServicesBootstrap.bootstrapServices();
    }

    @Override
    public ReadOnlyDatabases readOnlyDatabases()
    {
        return globalReadOnlyChecker;
    }
}
