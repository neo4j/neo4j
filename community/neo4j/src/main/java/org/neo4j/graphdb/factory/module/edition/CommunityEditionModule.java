/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.factory.module.edition;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.dbms.database.DatabaseContextProviderDelegate.delegate;
import static org.neo4j.dbms.routing.RoutingTableTTLProvider.ttlFromConfig;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.dbms.CommunityDatabaseStateService;
import org.neo4j.dbms.CommunityKernelPanicListener;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.DatabaseLifecycles;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.dbms.database.DatabaseReferenceCacheClearingListener;
import org.neo4j.dbms.database.DatabaseRepository;
import org.neo4j.dbms.database.DatabaseStateMonitor;
import org.neo4j.dbms.database.DefaultDatabaseContextFactory;
import org.neo4j.dbms.database.DefaultDatabaseContextFactoryComponents;
import org.neo4j.dbms.database.DefaultDatabaseDetailsExtrasProvider;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.dbms.database.DefaultTopologyInfoService;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.dbms.database.TopologyInfoService;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyListener;
import org.neo4j.dbms.identity.DefaultIdentityModule;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.identity.ServerIdentityFactory;
import org.neo4j.dbms.routing.ClientRoutingDomainChecker;
import org.neo4j.dbms.routing.DefaultDatabaseAvailabilityChecker;
import org.neo4j.dbms.routing.DefaultRoutingService;
import org.neo4j.dbms.routing.LocalRoutingTableServiceValidator;
import org.neo4j.dbms.routing.RoutingOption;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.dbms.routing.SingleAddressRoutingTableProvider;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphComponent;
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
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
import org.neo4j.kernel.impl.security.URIAccessRules;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.router.CommunityQueryRouterBootstrap;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.rest.repr.CommunityAuthConfigProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver;
import org.neo4j.ssl.config.SslPolicyLoader;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends AbstractEditionModule implements DefaultDatabaseContextFactoryComponents {
    protected final SslPolicyLoader sslPolicyLoader;
    protected final GlobalModule globalModule;
    protected final ServerIdentity identityModule;
    private final MapCachingDatabaseReferenceRepository databaseReferenceRepo;
    private final DeviceMapper deviceMapper;
    private final InternalLogProvider logProvider;
    private final CommunitySecurityLog securityLog;

    protected DatabaseStateService databaseStateService;
    protected ReadOnlyDatabases globalReadOnlyChecker;
    private Lifecycle defaultDatabaseInitializer = new LifecycleAdapter();
    private SystemGraphComponents systemGraphComponents;

    public CommunityEditionModule(GlobalModule globalModule) {
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        this.globalModule = globalModule;

        this.sslPolicyLoader =
                SslPolicyLoader.create(globalModule.getFileSystem(), globalConfig, logService.getInternalLogProvider());
        globalDependencies.satisfyDependency(sslPolicyLoader); // for bolt and web server
        globalDependencies.satisfyDependency(new DatabaseOperationCounts.Counter()); // for global metrics
        globalDependencies.satisfyDependency(new DatabaseStateMonitor.Counter()); // for global metrics

        globalDependencies.satisfyDependency(createAuthConfigProvider(globalModule));

        logProvider = globalModule.getLogService().getInternalLogProvider();
        securityLog = new CommunitySecurityLog(logProvider.getLog(CommunitySecurityModule.class));
        globalDependencies.satisfyDependency(new URIAccessRules(securityLog, globalConfig));

        identityModule = tryResolveOrCreate(
                        ServerIdentityFactory.class,
                        globalModule.getExternalDependencyResolver(),
                        DefaultIdentityModule::fromGlobalModule)
                .create(globalModule);
        globalDependencies.satisfyDependency(identityModule);

        deviceMapper = DeviceMapper.UNKNOWN_MAPPER;
        globalDependencies.satisfyDependency(deviceMapper);

        connectionTracker = globalDependencies.satisfyDependency(createConnectionTracker());
        databaseReferenceRepo = globalDependencies.satisfyDependency(new MapCachingDatabaseReferenceRepository());
    }

    @Override
    public DatabaseContextProvider<?> createDatabaseContextProvider(GlobalModule globalModule) {
        var databaseContextFactory = new DefaultDatabaseContextFactory(
                globalModule,
                identityModule,
                getTransactionMonitorFactory(),
                getIndexMonitorFactory(),
                createIdContextFactory(globalModule),
                deviceMapper,
                new CommunityIOControllerService(),
                createCommitProcessFactory(),
                this);

        var databaseIdRepo = new MapCachingDatabaseIdRepository();
        var databaseRepository = new DatabaseRepository<StandaloneDatabaseContext>(databaseIdRepo);
        var rootDatabaseIdRepository = AbstractEditionModule.tryResolveOrCreate(
                DatabaseIdRepository.class,
                globalModule.getExternalDependencyResolver(),
                () -> new SystemGraphDatabaseIdRepository(
                        () -> databaseRepository.getDatabaseContext(DatabaseId.SYSTEM_DATABASE_ID),
                        globalModule.getLogService().getInternalLogProvider()));
        var rootDatabaseReferenceRepository = AbstractEditionModule.tryResolveOrCreate(
                DatabaseReferenceRepository.class,
                globalModule.getExternalDependencyResolver(),
                () -> new SystemGraphDatabaseReferenceRepository(databaseRepository::getSystemDatabaseContext));
        databaseIdRepo.setDelegate(rootDatabaseIdRepository);
        databaseReferenceRepo.setDelegate(rootDatabaseReferenceRepository);
        var databaseIdCacheCleaner = new DatabaseReferenceCacheClearingListener(databaseIdRepo, databaseReferenceRepo);

        var kernelPanicListener =
                new CommunityKernelPanicListener(globalModule.getDatabaseEventListeners(), databaseRepository);
        globalModule.getGlobalLife().add(kernelPanicListener);

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
        globalModule.getGlobalDependencies().satisfyDependency(globalReadOnlyChecker);

        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, databaseIdCacheCleaner);

        defaultDatabaseInitializer = databaseLifecycles.defaultDatabaseStarter();

        globalModule
                .getGlobalDependencies()
                .satisfyDependency(SystemGraphComponents.UpgradeChecker.UPGRADE_ALWAYS_ALLOWED);

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
            TransactionManager transactionManager,
            Dependencies globalDependencies,
            Config config,
            InternalLogProvider userLogProvider,
            DbmsInfo dbmsInfo) {
        return new CommunityNeoWebServer(
                managementService,
                transactionManager,
                globalDependencies,
                config,
                userLogProvider,
                dbmsInfo,
                globalModule.getMemoryPools(),
                globalModule.getGlobalMonitors(),
                globalModule.getGlobalClock());
    }

    @Override
    public TopologyInfoService createTopologyInfoService(DatabaseContextProvider<?> databaseContextProvider) {
        var detailsExtrasProvider = new DefaultDatabaseDetailsExtrasProvider(databaseContextProvider);
        return new DefaultTopologyInfoService(
                identityModule.serverId(),
                globalModule.getGlobalConfig(),
                databaseStateService,
                globalReadOnlyChecker,
                detailsExtrasProvider);
    }

    @Override
    public RoutingService createRoutingService(
            DatabaseContextProvider<?> databaseContextProvider, ClientRoutingDomainChecker clientRoutingDomainChecker) {
        var logService = globalModule.getLogService();
        var portRegister = globalModule.getConnectorPortRegister();
        var config = globalModule.getGlobalConfig();
        var logProvider = globalModule.getLogService().getInternalLogProvider();
        var databaseAvailabilityChecker = new DefaultDatabaseAvailabilityChecker(databaseContextProvider);

        LocalRoutingTableServiceValidator validator =
                new LocalRoutingTableServiceValidator(databaseAvailabilityChecker);
        SingleAddressRoutingTableProvider routingTableProvider = new SingleAddressRoutingTableProvider(
                portRegister, RoutingOption.ROUTE_WRITE_AND_READ, config, logProvider, ttlFromConfig(config));

        return new DefaultRoutingService(
                logService.getInternalLogProvider(),
                validator,
                routingTableProvider,
                routingTableProvider,
                clientRoutingDomainChecker,
                config,
                () -> true,
                defaultDatabaseResolver,
                databaseReferenceRepo,
                true);
    }

    @Override
    public ProcedureConfig getProcedureConfig(Config config) {
        return new ProcedureConfig(config, false);
    }

    @Override
    protected AuthConfigProvider createAuthConfigProvider(GlobalModule globalModule) {
        return new CommunityAuthConfigProvider();
    }

    @Override
    public void registerDatabaseInitializers(GlobalModule globalModule, SystemDatabaseProvider systemDatabaseProvider) {
        registerSystemGraphInitializer(globalModule, systemDatabaseProvider);
        registerDefaultDatabaseInitializer(globalModule);
    }

    private void registerSystemGraphInitializer(
            GlobalModule globalModule, SystemDatabaseProvider systemDatabaseProvider) {
        var initializer = AbstractEditionModule.tryResolveOrCreate(
                SystemGraphInitializer.class,
                globalModule.getExternalDependencyResolver(),
                () -> new DefaultSystemGraphInitializer(systemDatabaseProvider::database, systemGraphComponents));
        globalModule.getGlobalDependencies().satisfyDependency(initializer);
        globalModule.getGlobalLife().add(initializer);
    }

    protected void registerDefaultDatabaseInitializer(GlobalModule globalModule) {
        globalModule.getGlobalLife().add(defaultDatabaseInitializer);
    }

    @Override
    public void registerSystemGraphComponents(
            SystemGraphComponents.Builder systemGraphComponentsBuilder, GlobalModule globalModule) {
        var config = globalModule.getGlobalConfig();
        var log = globalModule.getLogService().getInternalLogProvider();
        var clock = globalModule.getGlobalClock();
        var systemGraphComponent = new DefaultSystemGraphComponent(config, clock);
        var communityTopologyGraphComponentComponent = new CommunityTopologyGraphComponent(config, log);
        systemGraphComponentsBuilder.register(systemGraphComponent);
        systemGraphComponentsBuilder.register(communityTopologyGraphComponentComponent);
        registerSecurityGraphComponent(systemGraphComponentsBuilder, globalModule);
        this.systemGraphComponents = systemGraphComponentsBuilder.build();
    }

    private void registerSecurityGraphComponent(
            SystemGraphComponents.Builder systemGraphComponentsBuilder, GlobalModule globalModule) {
        var config = globalModule.getGlobalConfig();
        var fileSystem = globalModule.getFileSystem();

        var communityComponent = CommunitySecurityModule.createSecurityComponent(
                config,
                fileSystem,
                logProvider,
                securityLog,
                globalModule.getOtherMemoryPool().getPoolMemoryTracker());

        systemGraphComponentsBuilder.register(communityComponent);
        Dependencies dependencies = globalModule.getGlobalDependencies();
        dependencies.satisfyDependency(communityComponent);
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
        if (globalModule.getGlobalConfig().get(GraphDatabaseSettings.auth_enabled)) {
            SecurityModule securityModule = new CommunitySecurityModule(
                    globalModule.getLogService(),
                    globalModule.getGlobalConfig(),
                    globalModule.getGlobalDependencies(),
                    securityLog);
            securityModule.setup();
            return securityModule;
        }
        return NoAuthSecurityProvider.INSTANCE;
    }

    @Override
    public void createDefaultDatabaseResolver(SystemDatabaseProvider systemDatabaseProvider) {
        var defaultDatabaseResolver =
                new CommunityDefaultDatabaseResolver(globalModule.getGlobalConfig(), systemDatabaseProvider);
        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, defaultDatabaseResolver);
        this.defaultDatabaseResolver = defaultDatabaseResolver;
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider() {
        return globalModule.getGlobalDependencies().resolveDependency(BoltGraphDatabaseManagementServiceSPI.class);
    }

    protected CommitProcessFactory createCommitProcessFactory() {
        return new CommunityCommitProcessFactory();
    }

    @Override
    public void bootstrapQueryRouterServices(DatabaseManagementService databaseManagementService) {
        DatabaseContextProvider<? extends DatabaseContext> databaseRepository =
                globalModule.getGlobalDependencies().resolveDependency(DatabaseContextProvider.class);
        var queryRouterBootstrap = new CommunityQueryRouterBootstrap(
                globalModule.getGlobalLife(),
                globalModule.getGlobalDependencies(),
                globalModule.getLogService(),
                databaseRepository,
                databaseReferenceRepo,
                CommunitySecurityLog.NULL_LOG);
        globalModule
                .getGlobalDependencies()
                .satisfyDependency(queryRouterBootstrap.bootstrapServices(databaseManagementService));
    }

    @Override
    public ReadOnlyDatabases readOnlyDatabases() {
        return globalReadOnlyChecker;
    }

    @Override
    public SystemGraphComponents getSystemGraphComponents() {
        return systemGraphComponents;
    }
}
