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

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.dbms.database.DatabaseContextProviderDelegate.delegate;

import java.util.function.Supplier;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.dbms.CommunityDatabaseStateService;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.DatabaseInfoService;
import org.neo4j.dbms.database.DatabaseLifecycles;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.dbms.database.DatabaseReferenceCacheClearingListener;
import org.neo4j.dbms.database.DatabaseRepository;
import org.neo4j.dbms.database.DefaultDatabaseContextFactory;
import org.neo4j.dbms.database.DefaultDatabaseContextFactoryComponents;
import org.neo4j.dbms.database.DefaultDatabaseInfoService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.DetailedDbInfoProvider;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyListener;
import org.neo4j.dbms.identity.DefaultIdentityModule;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.identity.ServerIdentityFactory;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphComponent;
import org.neo4j.fabric.bootstrap.FabricServicesBootstrap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.MapCachingDatabaseIdRepository;
import org.neo4j.kernel.database.MapCachingDatabaseReferenceRepository;
import org.neo4j.kernel.database.SystemGraphDatabaseIdRepository;
import org.neo4j.kernel.database.SystemGraphDatabaseReferenceRepository;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.pagecache.CommunityIOControllerService;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.builtin.routing.AbstractRoutingProcedureInstaller;
import org.neo4j.procedure.builtin.routing.ClientRoutingDomainChecker;
import org.neo4j.procedure.builtin.routing.DefaultDatabaseAvailabilityChecker;
import org.neo4j.procedure.builtin.routing.SingleInstanceRoutingProcedureInstaller;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.rest.repr.CommunityAuthConfigProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends AbstractEditionModule implements DefaultDatabaseContextFactoryComponents {
    protected final SslPolicyLoader sslPolicyLoader;
    protected final GlobalModule globalModule;
    protected final ServerIdentity identityModule;
    private final MapCachingDatabaseReferenceRepository databaseReferenceRepo;
    private FabricServicesBootstrap fabricServicesBootstrap;

    protected DatabaseStateService databaseStateService;
    protected ReadOnlyDatabases globalReadOnlyChecker;
    private Lifecycle defaultDatabaseInitializer = new LifecycleAdapter();

    public CommunityEditionModule(GlobalModule globalModule) {
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        this.globalModule = globalModule;

        this.sslPolicyLoader =
                SslPolicyLoader.create(globalModule.getFileSystem(), globalConfig, logService.getInternalLogProvider());
        globalDependencies.satisfyDependency(sslPolicyLoader); // for bolt and web server
        globalDependencies.satisfyDependency(new DatabaseOperationCounts.Counter()); // for global metrics

        globalDependencies.satisfyDependency(createAuthConfigProvider(globalModule));

        identityModule = tryResolveOrCreate(
                        ServerIdentityFactory.class,
                        globalModule.getExternalDependencyResolver(),
                        DefaultIdentityModule::fromGlobalModule)
                .create(globalModule);
        globalDependencies.satisfyDependency(identityModule);

        connectionTracker = globalDependencies.satisfyDependency(createConnectionTracker());
        databaseReferenceRepo = new MapCachingDatabaseReferenceRepository();
    }

    @Override
    public DatabaseContextProvider<?> createDatabaseContextProvider(GlobalModule globalModule) {
        var databaseContextFactory = new DefaultDatabaseContextFactory(
                globalModule,
                getTransactionMonitorFactory(),
                createIdContextFactory(globalModule),
                new CommunityIOControllerService(),
                createCommitProcessFactory(),
                this);

        var databaseIdRepo = new MapCachingDatabaseIdRepository();
        var databaseRepository = new DatabaseRepository<StandaloneDatabaseContext>(databaseIdRepo);
        var rootDatabaseIdRepository = CommunityEditionModule.tryResolveOrCreate(
                DatabaseIdRepository.class,
                globalModule.getExternalDependencyResolver(),
                () -> new SystemGraphDatabaseIdRepository(databaseRepository::getSystemDatabaseContext));
        var rootDatabaseReferenceRepository = CommunityEditionModule.tryResolveOrCreate(
                DatabaseReferenceRepository.class,
                globalModule.getExternalDependencyResolver(),
                () -> new SystemGraphDatabaseReferenceRepository(databaseRepository::getSystemDatabaseContext));
        databaseIdRepo.setDelegate(rootDatabaseIdRepository);
        databaseReferenceRepo.setDelegate(rootDatabaseReferenceRepository);
        var databaseIdCacheCleaner = new DatabaseReferenceCacheClearingListener(databaseIdRepo, databaseReferenceRepo);

        fabricServicesBootstrap = new FabricServicesBootstrap.Community(
                globalModule.getGlobalLife(),
                globalModule.getGlobalDependencies(),
                globalModule.getLogService(),
                databaseRepository,
                databaseReferenceRepo);
        var databaseLifecycles = new DatabaseLifecycles(
                databaseRepository,
                globalModule.getGlobalConfig().get(initial_default_database),
                databaseContextFactory,
                globalModule.getLogService().getInternalLogProvider());
        databaseStateService = new CommunityDatabaseStateService(databaseRepository);

        globalModule.getGlobalLife().add(databaseLifecycles.systemDatabaseStarter());
        globalModule.getGlobalLife().add(databaseLifecycles.allDatabaseShutdown());
        globalModule.getGlobalDependencies().satisfyDependency(delegate(databaseRepository));
        globalModule.getGlobalDependencies().satisfyDependency(databaseStateService);

        globalReadOnlyChecker = createGlobalReadOnlyChecker(
                databaseRepository,
                globalModule.getGlobalConfig(),
                globalModule.getTransactionEventListeners(),
                globalModule.getGlobalLife(),
                globalModule.getLogService().getInternalLogProvider());

        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, databaseIdCacheCleaner);

        defaultDatabaseInitializer = databaseLifecycles.defaultDatabaseStarter();

        return databaseRepository;
    }

    private static ReadOnlyDatabases createGlobalReadOnlyChecker(
            DatabaseContextProvider<?> databaseContextProvider,
            Config globalConfig,
            GlobalTransactionEventListeners txListeners,
            LifeSupport globalLife,
            InternalLogProvider logProvider) {
        var systemGraphReadOnlyLookup =
                new SystemGraphReadOnlyDatabaseLookupFactory(databaseContextProvider, logProvider);
        var configReadOnlyLookup =
                new ConfigBasedLookupFactory(globalConfig, databaseContextProvider.databaseIdRepository());
        var globalChecker = new DefaultReadOnlyDatabases(systemGraphReadOnlyLookup, configReadOnlyLookup);
        var configListener = new ConfigReadOnlyDatabaseListener(globalChecker, globalConfig);
        var systemGraphListener = new SystemGraphReadOnlyListener(txListeners, globalChecker);
        globalLife.add(configListener);
        globalLife.add(systemGraphListener);
        return globalChecker;
    }

    @Override
    public Lifecycle createWebServer(
            DatabaseManagementService managementService,
            Dependencies globalDependencies,
            Config config,
            InternalLogProvider userLogProvider,
            DbmsInfo dbmsInfo) {
        return new CommunityNeoWebServer(
                managementService,
                globalDependencies,
                config,
                userLogProvider,
                dbmsInfo,
                globalModule.getMemoryPools(),
                globalModule.getTransactionManager(),
                globalModule.getGlobalClock());
    }

    @Override
    public DatabaseInfoService createDatabaseInfoService(DatabaseContextProvider<?> databaseContextProvider) {
        DetailedDbInfoProvider detailedDbInfoProvider = new DetailedDbInfoProvider(databaseContextProvider);
        var address = globalModule.getGlobalConfig().get(BoltConnector.advertised_address);
        return new DefaultDatabaseInfoService(
                identityModule.serverId(),
                address,
                databaseStateService,
                globalReadOnlyChecker,
                detailedDbInfoProvider);
    }

    @Override
    protected AbstractRoutingProcedureInstaller createRoutingProcedureInstaller(
            GlobalModule globalModule,
            DatabaseContextProvider<?> databaseContextProvider,
            ClientRoutingDomainChecker clientRoutingDomainChecker) {
        var portRegister = globalModule.getConnectorPortRegister();
        var config = globalModule.getGlobalConfig();
        var logProvider = globalModule.getLogService().getInternalLogProvider();
        var databaseAvailabilityChecker = new DefaultDatabaseAvailabilityChecker(databaseContextProvider);
        return new SingleInstanceRoutingProcedureInstaller(
                databaseAvailabilityChecker,
                databaseReferenceRepo,
                clientRoutingDomainChecker,
                portRegister,
                config,
                logProvider,
                defaultDatabaseResolver);
    }

    @Override
    protected AuthConfigProvider createAuthConfigProvider(GlobalModule globalModule) {
        return new CommunityAuthConfigProvider();
    }

    @Override
    public void registerSystemGraphInitializer(GlobalModule globalModule) {
        registerSystemGraphInitializer(globalModule, globalModule.getGlobalDependencies());
        registerDefaultDatabaseInitializer(globalModule);
    }

    protected void registerDefaultDatabaseInitializer(GlobalModule globalModule) {
        globalModule.getGlobalLife().add(defaultDatabaseInitializer);
    }

    @Override
    public void registerSystemGraphComponents(SystemGraphComponents systemGraphComponents, GlobalModule globalModule) {
        var config = globalModule.getGlobalConfig();
        var log = globalModule.getLogService().getInternalLogProvider();
        var clock = globalModule.getGlobalClock();
        var systemGraphComponent = new DefaultSystemGraphComponent(config, clock);
        systemGraphComponents.register(systemGraphComponent);
        var communityTopologyGraphComponentComponent = new CommunityTopologyGraphComponent(config, log);
        systemGraphComponents.register(communityTopologyGraphComponentComponent);
    }

    private void setupSecurityGraphInitializer(GlobalModule globalModule) {
        Config config = globalModule.getGlobalConfig();
        FileSystemAbstraction fileSystem = globalModule.getFileSystem();
        InternalLogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        AbstractSecurityLog securityLog = new CommunitySecurityLog(logProvider.getLog(CommunitySecurityModule.class));

        var communityComponent = CommunitySecurityModule.createSecurityComponent(
                config,
                fileSystem,
                logProvider,
                securityLog,
                globalModule.getOtherMemoryPool().getPoolMemoryTracker());

        Dependencies dependencies = globalModule.getGlobalDependencies();
        SystemGraphComponents systemGraphComponents = dependencies.resolveDependency(SystemGraphComponents.class);
        systemGraphComponents.register(communityComponent);
    }

    @Override
    public void createSecurityModule(GlobalModule globalModule) {
        setSecurityProvider(makeSecurityModule(globalModule));
    }

    @Override
    public DatabaseReferenceRepository getDatabaseReferenceRepo() {
        return databaseReferenceRepo;
    }

    private SecurityProvider makeSecurityModule(GlobalModule globalModule) {
        globalModule.getGlobalDependencies().satisfyDependency(CommunitySecurityLog.NULL_LOG);
        setupSecurityGraphInitializer(globalModule);
        if (globalModule.getGlobalConfig().get(GraphDatabaseSettings.auth_enabled)) {
            SecurityModule securityModule = new CommunitySecurityModule(
                    globalModule.getLogService(), globalModule.getGlobalConfig(), globalModule.getGlobalDependencies());
            securityModule.setup();
            return securityModule;
        }
        return NoAuthSecurityProvider.INSTANCE;
    }

    @Override
    public void createDefaultDatabaseResolver(GlobalModule globalModule) {
        Supplier<GraphDatabaseService> systemDbSupplier = systemSupplier(globalModule.getGlobalDependencies());
        CommunityDefaultDatabaseResolver defaultDatabaseResolver =
                new CommunityDefaultDatabaseResolver(globalModule.getGlobalConfig(), systemDbSupplier);
        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, defaultDatabaseResolver);
        setDefaultDatabaseResolver(defaultDatabaseResolver);
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider(
            Dependencies dependencies,
            DatabaseManagementService managementService,
            Monitors monitors,
            SystemNanoClock clock,
            LogService logService) {
        var kernelDatabaseManagementService =
                createBoltKernelDatabaseManagementServiceProvider(dependencies, managementService, monitors, clock);
        return fabricServicesBootstrap.createBoltDatabaseManagementServiceProvider(
                kernelDatabaseManagementService, managementService, monitors, clock);
    }

    protected static BoltGraphDatabaseManagementServiceSPI createBoltKernelDatabaseManagementServiceProvider(
            Dependencies dependencies,
            DatabaseManagementService managementService,
            Monitors monitors,
            SystemNanoClock clock) {
        var config = dependencies.resolveDependency(Config.class);
        var bookmarkAwaitDuration = config.get(GraphDatabaseSettings.bookmark_ready_timeout);
        return new BoltKernelDatabaseManagementServiceProvider(
                managementService, monitors, clock, bookmarkAwaitDuration);
    }

    protected CommitProcessFactory createCommitProcessFactory() {
        return new CommunityCommitProcessFactory();
    }

    @Override
    public void bootstrapFabricServices() {
        fabricServicesBootstrap.bootstrapServices();
    }

    @Override
    public ReadOnlyDatabases readOnlyDatabases() {
        return globalReadOnlyChecker;
    }
}
