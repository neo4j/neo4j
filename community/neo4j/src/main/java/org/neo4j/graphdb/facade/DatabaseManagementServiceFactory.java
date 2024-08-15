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
package org.neo4j.graphdb.facade;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.factory.module.edition.CommunityEditionModule.tryResolveOrCreate;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTGeometry;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPoint;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import io.netty.util.internal.logging.InternalLoggerFactory;
import java.lang.reflect.RecordComponent;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.bolt.BoltServer;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.transport.Netty4LoggerFactory;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.bolt.tx.TransactionManagerImpl;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.admissioncontrol.AdmissionControlService;
import org.neo4j.dbms.admissioncontrol.NoopAdmissionControlService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.DatabaseManagementServiceImpl;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.UnableToStartDatabaseException;
import org.neo4j.dbms.routing.ClientRoutingDomainChecker;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.dbms.systemgraph.ContextBasedSystemDatabaseProvider;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.collector.DataCollector;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.database.DatabaseSizeServiceImpl;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.StatusDetailsAccessor;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.procedure.builtin.SpecialBuiltInProcedures;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.procedure.impl.ProcedureGraphDatabaseAPI;
import org.neo4j.procedure.impl.ProcedureLoginContextTransformer;
import org.neo4j.procedure.impl.TerminationGuardProvider;
import org.neo4j.procedure.impl.TransactionStatusDetailsProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.DisabledNeoWebServer;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

/**
 * This is the main factory for creating database instances. It delegates creation to two different modules
 * ({@link GlobalModule} and {@link AbstractEditionModule}),
 * which create all the specific services needed to run a graph database.
 * <p>
 * To create test versions of databases, override an edition factory (e.g. {@link org.neo4j.kernel.impl.factory
 * .CommunityFacadeFactory}), and replace modules
 * with custom versions that instantiate alternative services.
 */
public class DatabaseManagementServiceFactory {
    protected final DbmsInfo dbmsInfo;
    private final Function<GlobalModule, AbstractEditionModule> editionFactory;

    public DatabaseManagementServiceFactory(
            DbmsInfo dbmsInfo, Function<GlobalModule, AbstractEditionModule> editionFactory) {
        this.dbmsInfo = dbmsInfo;
        this.editionFactory = editionFactory;
    }

    /**
     * Instantiate a graph database given configuration, dependencies, and a custom implementation of
     * {@link org .neo4j.kernel.impl.factory.GraphDatabaseFacade}.
     *
     * @param config       configuration
     * @param daemonMode   if running in daemon mode. If {@code true}, we avoid printing to stdout/stderr.
     * @param dependencies the dependencies required to construct the {@link GraphDatabaseFacade}
     * @return the initialised {@link GraphDatabaseFacade}
     */
    public DatabaseManagementService build(Config config, boolean daemonMode, final ExternalDependencies dependencies) {
        var globalModule = createGlobalModule(config, daemonMode, dependencies);
        var edition = editionFactory.apply(globalModule);
        var globalDependencies = globalModule.getGlobalDependencies();
        var globalLife = globalModule.getGlobalLife();
        var logService = globalModule.getLogService();
        var internalLog = logService.getInternalLog(getClass());

        var dbmsRuntimeSystemGraphComponent = new DbmsRuntimeSystemGraphComponent(globalModule.getGlobalConfig());
        var systemGraphComponentsBuilder = tryResolveOrCreate(
                SystemGraphComponents.Builder.class,
                globalModule.getExternalDependencyResolver(),
                SystemGraphComponents.DefaultBuilder::new);
        systemGraphComponentsBuilder.register(dbmsRuntimeSystemGraphComponent);
        edition.registerSystemGraphComponents(systemGraphComponentsBuilder, globalModule);
        globalDependencies.satisfyDependency(edition.getSystemGraphComponents());

        var databaseContextProvider = edition.createDatabaseContextProvider(globalModule);
        var systemDatabaseProvider = new ContextBasedSystemDatabaseProvider(databaseContextProvider);
        edition.createGlobalReadOnlyChecker(
                systemDatabaseProvider, databaseContextProvider.databaseIdRepository(), globalModule);
        var managementService = createManagementService(globalModule, globalLife, internalLog, databaseContextProvider);
        globalDependencies.satisfyDependencies(managementService);
        globalDependencies.satisfyDependency(new DatabaseSizeServiceImpl(databaseContextProvider));
        var topologyInfoService = edition.createTopologyInfoService(databaseContextProvider);
        globalDependencies.satisfyDependencies(topologyInfoService);

        // Routing procedures depend on DatabaseResolver
        edition.createDefaultDatabaseResolver(systemDatabaseProvider);
        globalDependencies.satisfyDependency(edition.getDefaultDatabaseResolver());

        var clientRoutingDomainChecker = tryResolveOrCreate(
                ClientRoutingDomainChecker.class,
                globalModule.getGlobalDependencies(),
                () -> edition.createClientRoutingDomainChecker(globalModule));

        var routingService = edition.createRoutingService(databaseContextProvider, clientRoutingDomainChecker);
        globalDependencies.satisfyDependency(routingService);

        // Fabric depends on Procedures
        setupProcedures(globalModule, edition, databaseContextProvider, routingService);

        edition.bootstrapQueryRouterServices(managementService);

        edition.registerDatabaseInitializers(globalModule, systemDatabaseProvider);

        edition.createSecurityModule(globalModule);
        SecurityProvider securityProvider = edition.getSecurityProvider();
        globalDependencies.satisfyDependencies(securityProvider.authManager());

        var dbmsRuntimeVersionProvider = edition.createAndRegisterDbmsRuntimeRepository(
                globalModule, databaseContextProvider, globalDependencies, dbmsRuntimeSystemGraphComponent);
        globalDependencies.satisfyDependency(dbmsRuntimeVersionProvider);

        globalLife.add(globalModule.getGlobalExtensions());
        BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI =
                edition.createBoltDatabaseManagementServiceProvider();

        var transactionManager =
                new TransactionManagerImpl(boltGraphDatabaseManagementServiceSPI, globalModule.getGlobalClock());
        globalDependencies.satisfyDependency(transactionManager);

        var acs =
                tryResolveOrCreate(AdmissionControlService.class, globalDependencies, NoopAdmissionControlService::new);
        var boltServer = createBoltServer(globalModule, edition, transactionManager, routingService, config, acs);

        globalLife.add(boltServer);
        globalDependencies.satisfyDependency(boltServer);
        var webServer = createWebServer(
                edition,
                managementService,
                globalDependencies,
                transactionManager,
                config,
                globalModule.getLogService().getUserLogProvider());
        globalDependencies.satisfyDependency(webServer);
        globalLife.add(webServer);

        globalLife.add(globalModule.getCapabilitiesService());

        startDatabaseServer(globalModule, globalLife, internalLog, databaseContextProvider, managementService);

        // System is available here, checked on startDatabaseServer
        dumpDbmsInfo(
                logService.getUserLog(getClass()),
                databaseContextProvider
                        .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                        .get()
                        .databaseFacade());

        return managementService;
    }

    protected DatabaseManagementService createManagementService(
            GlobalModule globalModule,
            LifeSupport globalLife,
            InternalLog internalLog,
            DatabaseContextProvider<?> databaseContextProvider) {
        return new DatabaseManagementServiceImpl(
                databaseContextProvider,
                globalLife,
                globalModule.getDatabaseEventListeners(),
                globalModule.getTransactionEventListeners(),
                internalLog,
                globalModule.getGlobalConfig());
    }

    private Lifecycle createWebServer(
            AbstractEditionModule edition,
            DatabaseManagementService managementService,
            Dependencies globalDependencies,
            TransactionManager transactionManager,
            Config config,
            InternalLogProvider userLogProvider) {
        if (shouldEnableWebServer(config)) {
            return edition.createWebServer(
                    managementService, transactionManager, globalDependencies, config, userLogProvider, dbmsInfo);
        }
        return new DisabledNeoWebServer();
    }

    private static boolean shouldEnableWebServer(Config config) {
        return (config.get(HttpConnector.enabled) || config.get(HttpsConnector.enabled))
                && !config.get(ServerSettings.http_enabled_modules).isEmpty()
                && !config.get(ServerSettings.http_enabled_transports).isEmpty();
    }

    private static void startDatabaseServer(
            GlobalModule globalModule,
            LifeSupport globalLife,
            InternalLog internalLog,
            DatabaseContextProvider<?> databaseContextProvider,
            DatabaseManagementService managementService) {

        RuntimeException startupException = null;
        try {
            globalLife.start();

            DatabaseStateService databaseStateService =
                    globalModule.getGlobalDependencies().resolveDependency(DatabaseStateService.class);

            verifySystemDatabaseStart(databaseContextProvider, databaseStateService);
        } catch (Throwable throwable) {
            String message = "Error starting Neo4j database server at "
                    + globalModule.getNeo4jLayout().databasesDirectory();
            startupException = new RuntimeException(message, throwable);
            internalLog.error(message, throwable);
        } finally {
            if (startupException != null) {
                try {
                    managementService.shutdown();
                } catch (Throwable shutdownError) {
                    startupException.addSuppressed(shutdownError);
                }
            }
        }

        if (startupException != null) {
            internalLog.error("Failed to start database server.", startupException);
            throw startupException;
        }
    }

    private static void verifySystemDatabaseStart(
            DatabaseContextProvider<?> databaseContextProvider, DatabaseStateService dbStateService) {
        Optional<? extends DatabaseContext> databaseContext =
                databaseContextProvider.getDatabaseContext(NAMED_SYSTEM_DATABASE_ID);
        if (databaseContext.isEmpty()) {
            throw new UnableToStartDatabaseException(SYSTEM_DATABASE_NAME + " not found.");
        }

        Optional<Throwable> failure = dbStateService.causeOfFailure(NAMED_SYSTEM_DATABASE_ID);
        if (failure.isPresent()) {
            throw new UnableToStartDatabaseException(SYSTEM_DATABASE_NAME + " failed to start.", failure.get());
        }
    }

    /**
     * Create the platform module. Override to replace with custom module.
     */
    protected GlobalModule createGlobalModule(
            Config config, boolean daemonMode, final ExternalDependencies dependencies) {
        return new GlobalModule(config, dbmsInfo, daemonMode, dependencies);
    }

    /**
     * Creates and registers the systems procedures, including those which belong to a particular edition.
     * N.B. This method takes a {@link DatabaseContextProvider} as an unused parameter *intentionally*, in
     * order to enforce that the databaseManager must be constructed first.
     */
    @SuppressWarnings("unused")
    private static void setupProcedures(
            GlobalModule globalModule,
            AbstractEditionModule editionModule,
            DatabaseContextProvider<?> databaseContextProvider,
            RoutingService routingService) {

        Supplier<GlobalProcedures> procedureInitializer = () -> {
            Config globalConfig = globalModule.getGlobalConfig();
            Path proceduresDirectory = globalConfig.get(GraphDatabaseSettings.plugin_dir);
            LogService logService = globalModule.getLogService();
            InternalLog internalLog = logService.getInternalLog(GlobalProcedures.class);
            Log proceduresLog = logService.getUserLog(GlobalProcedures.class);

            ProcedureConfig procedureConfig = editionModule.getProcedureConfig(globalConfig);
            Edition neo4jEdition = globalModule.getDbmsInfo().edition;
            SpecialBuiltInProcedures builtInProcedures =
                    SpecialBuiltInProcedures.from(Version.getNeo4jVersion(), neo4jEdition.toString());
            GlobalProceduresRegistry globalProcedures =
                    new GlobalProceduresRegistry(builtInProcedures, proceduresDirectory, internalLog, procedureConfig);

            try (var registry = globalProcedures.bulk()) {
                registry.registerType(Node.class, NTNode);
                registry.registerType(NodeValue.class, NTNode);
                registry.registerType(Relationship.class, NTRelationship);
                registry.registerType(RelationshipValue.class, NTRelationship);
                registry.registerType(org.neo4j.graphdb.Path.class, NTPath);
                registry.registerType(PathValue.class, NTPath);
                registry.registerType(Geometry.class, NTGeometry);
                registry.registerType(Point.class, NTPoint);
                registry.registerType(PointValue.class, NTPoint);

                // Below components are not public API, but are made available for internal
                // procedures to call, and to provide temporary workarounds for the following
                // patterns:
                //  - Batch-transaction imports (GDAPI, needs to be real and passed to background processing threads)
                //  - Group-transaction writes (same pattern as above, but rather than splitting large transactions,
                //                              combine lots of small ones)
                //  - Bleeding-edge performance (KernelTransaction, to bypass overhead of working with Core API)
                registry.registerComponent(DependencyResolver.class, Context::dependencyResolver, false);
                registry.registerComponent(KernelTransaction.class, Context::kernelTransaction, false);
                registry.registerComponent(GraphDatabaseAPI.class, Context::graphDatabaseAPI, false);
                registry.registerComponent(
                        SystemGraphComponents.class, ctx -> editionModule.getSystemGraphComponents(), false);
                registry.registerComponent(
                        DataCollector.class,
                        ctx -> ctx.dependencyResolver().resolveDependency(DataCollector.class),
                        false);
                registry.registerComponent(
                        KernelVersion.class,
                        ctx -> ctx.dependencyResolver()
                                .resolveDependency(KernelVersionProvider.class)
                                .kernelVersion(),
                        false);

                // Register injected public API components
                registry.registerComponent(Log.class, ctx -> proceduresLog, true);
                registry.registerComponent(Transaction.class, Context::transaction, true);
                registry.registerComponent(
                        org.neo4j.procedure.TerminationGuard.class, new TerminationGuardProvider(), true);
                registry.registerComponent(StatusDetailsAccessor.class, new TransactionStatusDetailsProvider(), true);
                registry.registerComponent(SecurityContext.class, Context::securityContext, true);
                registry.registerComponent(URLAccessChecker.class, Context::urlAccessChecker, true);
                registry.registerComponent(ProcedureCallContext.class, Context::procedureCallContext, true);
                registry.registerComponent(
                        FulltextAdapter.class,
                        ctx -> ctx.dependencyResolver().resolveDependency(FulltextAdapter.class),
                        true);
                registry.registerComponent(
                        GraphDatabaseService.class,
                        ctx -> new ProcedureGraphDatabaseAPI(
                                ctx.graphDatabaseAPI(),
                                new ProcedureLoginContextTransformer(ctx),
                                ctx.dependencyResolver().resolveDependency(Config.class)),
                        true);
                registry.registerComponent(ValueMapper.class, Context::valueMapper, true);

                // Edition procedures
                try {
                    editionModule.registerProcedures(
                            registry, procedureConfig, globalModule, databaseContextProvider, routingService);
                } catch (KernelException e) {
                    internalLog.error("Failed to register built-in edition procedures at start up: " + e.getMessage());
                }
            }
            globalModule.getGlobalLife().add(globalProcedures);

            return globalProcedures;
        };
        GlobalProcedures procedures = tryResolveOrCreate(
                GlobalProcedures.class, globalModule.getExternalDependencyResolver(), procedureInitializer);
        if (procedures instanceof Consumer) {
            ((Consumer) procedures).accept(procedureInitializer);
        }
        globalModule.getGlobalDependencies().satisfyDependency(procedures);
    }

    private static BoltServer createBoltServer(
            GlobalModule globalModule,
            AbstractEditionModule edition,
            TransactionManager transactionManager,
            RoutingService routingService,
            Config config,
            AdmissionControlService admissionControlService) {

        // Must be called before loading any Netty classes in order to override the factory
        InternalLoggerFactory.setDefaultFactory(
                new Netty4LoggerFactory(globalModule.getLogService().getInternalLogProvider()));

        if (config.get(BoltConnectorInternalSettings.local_channel_address) == null) {
            config.set(
                    BoltConnectorInternalSettings.local_channel_address,
                    UUID.randomUUID().toString());
        }

        return new BoltServer(
                globalModule.getDbmsInfo(),
                globalModule.getJobScheduler(),
                globalModule.getConnectorPortRegister(),
                edition.getConnectionTracker(),
                transactionManager,
                globalModule.getGlobalConfig(),
                globalModule.getGlobalClock(),
                globalModule.getGlobalMonitors(),
                globalModule.getLogService(),
                globalModule.getGlobalDependencies(),
                edition.getBoltAuthManager(globalModule.getGlobalDependencies()),
                edition.getBoltInClusterAuthManager(),
                edition.getBoltLoopbackAuthManager(),
                globalModule.getMemoryPools(),
                routingService,
                edition.getDefaultDatabaseResolver(),
                admissionControlService);
    }

    private static void dumpDbmsInfo(InternalLog log, GraphDatabaseAPI system) {
        try {
            for (BuiltInDbmsProcedures.SystemInfo info :
                    BuiltInDbmsProcedures.dbmsInfo(system).toList()) {
                for (RecordComponent recordComponent : BuiltInDbmsProcedures.SystemInfo.class.getRecordComponents()) {
                    log.info(recordComponent.getName() + ": "
                            + recordComponent.getAccessor().invoke(info));
                }
            }
        } catch (Exception e) {
            log.info("Unable to dump DBMS information", e);
        }
    }
}
