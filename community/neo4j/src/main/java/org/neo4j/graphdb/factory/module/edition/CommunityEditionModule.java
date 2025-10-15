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

import java.util.Set;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.CommunityDatabaseStateService;
import org.neo4j.dbms.CommunityKernelPanicListener;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.CommunityDatabaseObjectRepositoryModelProvider;
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
import org.neo4j.dbms.database.readonly.ReadOnlyChangeListener;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyListener;
import org.neo4j.dbms.identity.DefaultIdentityModule;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.identity.ServerIdentityFactory;
import org.neo4j.dbms.routing.ClientRoutingDomainChecker;
import org.neo4j.dbms.routing.CommunityRoutingService;
import org.neo4j.dbms.routing.DefaultDatabaseAvailabilityChecker;
import org.neo4j.dbms.routing.DefaultRoutingService;
import org.neo4j.dbms.routing.LocalRoutingTableServiceValidator;
import org.neo4j.dbms.routing.RoutingOption;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.dbms.routing.SingleAddressRoutingTableProvider;
import org.neo4j.dbms.systemgraph.CommunityDefaultQueryLanguageLookup;
import org.neo4j.dbms.systemgraph.CommunityTopologyGraphComponent;
import org.neo4j.dbms.systemgraph.ContextBasedSystemDatabaseProvider;
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.database.MapCachingDatabaseIdRepository;
import org.neo4j.kernel.database.MapCachingDatabaseReferenceRepository;
import org.neo4j.kernel.database.ModelBasedDatabaseIdRepository;
import org.neo4j.kernel.database.ModelBasedDatabaseReferenceRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.TransactionalProcessFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.DefaultTransactionalProcessFactory;
import org.neo4j.kernel.impl.pagecache.CommunityIOControllerService;
import org.neo4j.kernel.impl.security.URIAccessRules;
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
import org.neo4j.ssl.config.DefaultSslPolicyProvider;

/**
 * This implementation of {@link AbstractEditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends AbstractEditionModule implements DefaultDatabaseContextFactoryComponents {
    protected final GlobalModule globalModule;
    protected final ServerIdentity identityModule;
    private final DeviceMapper deviceMapper;
    private final InternalLogProvider logProvider;
    private final CommunitySecurityLog securityLog;

    protected DatabaseStateService databaseStateService;
    private MapCachingDatabaseReferenceRepository databaseReferenceRepo;
    private Lifecycle defaultDatabaseInitializer = new LifecycleAdapter();
    private SystemGraphComponents systemGraphComponents;

    public CommunityEditionModule(GlobalModule globalModule) {
        Dependencies globalDependencies = globalModule.getGlobalDependencies();
        Config globalConfig = globalModule.getGlobalConfig();
        LogService logService = globalModule.getLogService();
        this.globalModule = globalModule;

        var sslPolicyProvider = new DefaultSslPolicyProvider(
                globalModule.getFileSystem(), globalConfig, true, logService.getInternalLogProvider());
        globalDependencies.satisfyDependency(sslPolicyProvider); // for bolt and web server
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
        var systemDatabaseProvider =
                new ContextBasedSystemDatabaseProvider(databaseRepository, globalModule.getDatabaseEventListeners());
        var modelProvider = new CommunityDatabaseObjectRepositoryModelProvider(systemDatabaseProvider);

        databaseIdRepo.setDelegate(AbstractEditionModule.tryResolveOrCreate(
                DatabaseIdRepository.class,
                globalModule.getExternalDependencyResolver(),
                () -> new ModelBasedDatabaseIdRepository(modelProvider)));
        databaseReferenceRepo = globalModule
                .getGlobalDependencies()
                .satisfyDependency(new MapCachingDatabaseReferenceRepository(AbstractEditionModule.tryResolveOrCreate(
                        DatabaseReferenceRepository.class,
                        globalModule.getExternalDependencyResolver(),
                        () -> new ModelBasedDatabaseReferenceRepository(modelProvider))));
        var databaseIdCacheCleaner = new DatabaseReferenceCacheClearingListener(databaseIdRepo, databaseReferenceRepo);

        var kernelPanicListener =
                new CommunityKernelPanicListener(globalModule.getDatabaseEventListeners(), databaseRepository);
        globalModule.getGlobalLife().add(kernelPanicListener);

        var databaseLifecycles = new DatabaseLifecycles(
                databaseRepository,
                globalModule.getGlobalConfig().get(initial_default_database),
                databaseContextFactory,
                globalModule.getLogService().getInternalLogProvider(),
                globalModule.getExceptionHandlerService());
        databaseStateService = new CommunityDatabaseStateService(databaseRepository);

        globalModule.getGlobalLife().add(databaseLifecycles.systemDatabaseStarter());
        globalModule.getGlobalLife().add(databaseLifecycles.allDatabaseShutdown());
        globalModule.getGlobalDependencies().satisfyDependency(delegate(databaseRepository));
        globalModule.getGlobalDependencies().satisfyDependency(databaseStateService);

        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, databaseIdCacheCleaner);

        defaultDatabaseInitializer = databaseLifecycles.defaultDatabaseStarter();

        globalModule
                .getGlobalDependencies()
                .satisfyDependency(SystemGraphComponents.UpgradeChecker.UPGRADE_ALWAYS_ALLOWED);

        var defaultQueryLanguage = new CommunityDefaultQueryLanguageLookup(
                systemDatabaseProvider, globalModule.getJobScheduler(), logProvider);
        globalModule.getGlobalDependencies().satisfyDependency(defaultQueryLanguage);
        globalModule.getGlobalLife().add(defaultQueryLanguage.life());
        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, defaultQueryLanguage.transactionListener());

        return databaseRepository;
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
        var config = globalModule.getGlobalConfig();
        if (config.get(GraphDatabaseInternalSettings.use_new_routing_stack)) {
            return new CommunityRoutingService(
                    databaseContextProvider,
                    defaultDatabaseResolver,
                    globalModule.getConnectorPortRegister(),
                    globalModule.getGlobalConfig());
        }
        var logService = globalModule.getLogService();
        var portRegister = globalModule.getConnectorPortRegister();
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
                true,
                globalModule.getGlobalClock());
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
    public void createSecurityModule(GlobalModule globalModule, SystemDatabaseProvider systemDatabaseProvider) {
        setSecurityProvider(makeSecurityModule(globalModule));
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
        this.defaultDatabaseResolver =
                DefaultDatabaseResolver.constant(globalModule.getGlobalConfig().get(initial_default_database));
    }

    @Override
    public void createGlobalReadOnlyChecker(
            SystemDatabaseProvider systemDatabaseProvider,
            DatabaseIdRepository databaseIdRepository,
            GlobalModule globalModule) {
        globalReadOnlyChecker = createGlobalReadOnlyChecker(
                Set.of(SystemGraphReadOnlyDatabaseLookupFactory.DEFAULT_PROVIDER),
                systemDatabaseProvider,
                name -> databaseIdRepository.getByName(name).map(NamedDatabaseId::databaseId),
                ReadOnlyChangeListener.NO_OP,
                globalModule);
        globalModule
                .getGlobalLife()
                .add(new SystemGraphReadOnlyListener(
                        globalModule.getTransactionEventListeners(), globalReadOnlyChecker));
        globalModule.getGlobalDependencies().satisfyDependency(globalReadOnlyChecker);
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider() {
        return globalModule.getGlobalDependencies().resolveDependency(BoltGraphDatabaseManagementServiceSPI.class);
    }

    protected TransactionalProcessFactory createCommitProcessFactory() {
        return new DefaultTransactionalProcessFactory();
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
