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
import static org.neo4j.procedure.impl.temporal.TemporalFunction.registerTemporalFunctions;

import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.StandaloneDbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.TopologyInfoService;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.ReadOnlyChangeListener;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory;
import org.neo4j.dbms.routing.ClientRoutingDomainChecker;
import org.neo4j.dbms.routing.RoutingOption;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.dbms.routing.RoutingTableTTLProvider;
import org.neo4j.dbms.routing.ServerSideRoutingTableProvider;
import org.neo4j.dbms.routing.SimpleClientRoutingDomainChecker;
import org.neo4j.dbms.routing.SingleAddressRoutingTableProvider;
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryProvider;
import org.neo4j.internal.collector.DataCollectorProcedures;
import org.neo4j.kernel.api.net.DefaultNetworkConnectionTracker;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.procedure.builtin.BuiltInProcedures;
import org.neo4j.procedure.builtin.FulltextProcedures;
import org.neo4j.procedure.builtin.TokenProcedures;
import org.neo4j.procedure.builtin.VectorIndexProcedures;
import org.neo4j.procedure.builtin.graphschema.Introspect;
import org.neo4j.procedure.builtin.routing.RoutingProcedureInstaller;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.util.FeatureToggles;

/**
 * Edition module for {@link DatabaseManagementServiceFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
public abstract class AbstractEditionModule {
    protected NetworkConnectionTracker connectionTracker;
    protected SecurityProvider securityProvider;
    protected DefaultDatabaseResolver defaultDatabaseResolver;
    protected ReadOnlyDatabases globalReadOnlyChecker;

    public void registerProcedures(
            GlobalProcedures globalProcedures,
            ProcedureConfig procedureConfig,
            GlobalModule globalModule,
            DatabaseContextProvider<?> databaseContextProvider,
            RoutingService routingService)
            throws KernelException {
        globalProcedures.registerProcedure(BuiltInProcedures.class);
        globalProcedures.registerProcedure(TokenProcedures.class);
        globalProcedures.registerProcedure(BuiltInDbmsProcedures.class);
        globalProcedures.registerProcedure(FulltextProcedures.class);
        globalProcedures.registerProcedure(VectorIndexProcedures.class);
        globalProcedures.registerProcedure(DataCollectorProcedures.class);
        if (FeatureToggles.flag(Introspect.class, "enabled", false)) {
            globalProcedures.registerProcedure(Introspect.class);
        }
        registerTemporalFunctions(globalProcedures, procedureConfig);

        registerEditionSpecificProcedures(globalProcedures, databaseContextProvider);
        RoutingProcedureInstaller.install(
                globalProcedures, routingService, globalModule.getLogService().getInternalLogProvider());
    }

    public ClientRoutingDomainChecker createClientRoutingDomainChecker(GlobalModule globalModule) {
        Config config = globalModule.getGlobalConfig();
        var domainChecker = SimpleClientRoutingDomainChecker.fromConfig(
                config, globalModule.getLogService().getInternalLogProvider());
        globalModule.getGlobalDependencies().satisfyDependencies(domainChecker);
        return domainChecker;
    }

    protected void registerEditionSpecificProcedures(
            GlobalProcedures globalProcedures, DatabaseContextProvider<?> databaseContextProvider)
            throws KernelException {}

    protected abstract AuthConfigProvider createAuthConfigProvider(GlobalModule globalModule);

    public abstract <DB extends DatabaseContext> DatabaseContextProvider<DB> createDatabaseContextProvider(
            GlobalModule globalModule);

    public abstract void registerDatabaseInitializers(
            GlobalModule globalModule, SystemDatabaseProvider systemDatabaseProvider);

    public abstract void registerSystemGraphComponents(
            SystemGraphComponents.Builder systemGraphComponentsBuilder, GlobalModule globalModule);

    public abstract SystemGraphComponents getSystemGraphComponents();

    public abstract void createSecurityModule(GlobalModule globalModule);

    public abstract DatabaseReferenceRepository getDatabaseReferenceRepo();

    public abstract void createGlobalReadOnlyChecker(
            SystemDatabaseProvider systemDatabaseProvider,
            DatabaseIdRepository databaseIdRepository,
            GlobalModule globalModule);

    protected static ReadOnlyDatabases createGlobalReadOnlyChecker(
            Set<SystemGraphReadOnlyDatabaseLookupFactory.ReadonlyDatabasesProvider> readOnlyDatabaseProviders,
            SystemDatabaseProvider systemDatabaseProvider,
            DatabaseIdRepository databaseIdRepository,
            ReadOnlyChangeListener listener,
            GlobalModule globalModule) {
        var globalConfig = globalModule.getGlobalConfig();
        var logProvider = globalModule.getLogService().getInternalLogProvider();
        var systemGraphReadOnlyLookup = new SystemGraphReadOnlyDatabaseLookupFactory(
                systemDatabaseProvider, logProvider, readOnlyDatabaseProviders);
        var configReadOnlyLookup = new ConfigBasedLookupFactory(globalConfig, databaseIdRepository);
        var globalReadOnlyChecker =
                new DefaultReadOnlyDatabases(listener, systemGraphReadOnlyLookup, configReadOnlyLookup);
        var configListener = new ConfigReadOnlyDatabaseListener(globalReadOnlyChecker, globalConfig);
        globalModule.getGlobalLife().add(configListener);
        return globalReadOnlyChecker;
    }

    protected static NetworkConnectionTracker createConnectionTracker() {
        return new DefaultNetworkConnectionTracker();
    }

    public DatabaseTransactionStats.Factory getTransactionMonitorFactory() {
        return DatabaseTransactionStats::new;
    }

    public DatabaseIndexStats.Factory getIndexMonitorFactory() {
        return DatabaseIndexStats::new;
    }

    public NetworkConnectionTracker getConnectionTracker() {
        return connectionTracker;
    }

    public SecurityProvider getSecurityProvider() {
        return securityProvider;
    }

    public void setSecurityProvider(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    public abstract void createDefaultDatabaseResolver(SystemDatabaseProvider systemDatabaseProvider);

    public DefaultDatabaseResolver getDefaultDatabaseResolver() {
        return defaultDatabaseResolver;
    }

    public abstract void bootstrapQueryRouterServices(DatabaseManagementService databaseManagementService);

    public abstract BoltGraphDatabaseManagementServiceSPI createBoltDatabaseManagementServiceProvider();

    public AuthManager getBoltAuthManager(DependencyResolver dependencyResolver) {
        return dependencyResolver.resolveDependency(AuthManager.class);
    }

    public AuthManager getBoltInClusterAuthManager() {
        return securityProvider.inClusterAuthManager();
    }

    public AuthManager getBoltLoopbackAuthManager() {
        return securityProvider.loopbackAuthManager();
    }

    public abstract Lifecycle createWebServer(
            DatabaseManagementService managementService,
            TransactionManager transactionManager,
            Dependencies globalDependencies,
            Config config,
            InternalLogProvider userLogProvider,
            DbmsInfo dbmsInfo);

    public DbmsRuntimeVersionProvider createAndRegisterDbmsRuntimeRepository(
            GlobalModule globalModule,
            DatabaseContextProvider<?> databaseContextProvider,
            Dependencies dependencies,
            DbmsRuntimeSystemGraphComponent dbmsRuntimeSystemGraphComponent) {
        var dbmsRuntimeRepository =
                new StandaloneDbmsRuntimeVersionProvider(databaseContextProvider, dbmsRuntimeSystemGraphComponent);
        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, dbmsRuntimeRepository);
        return dbmsRuntimeRepository;
    }

    protected ServerSideRoutingTableProvider serverSideRoutingTableProvider(GlobalModule globalModule) {
        ConnectorPortRegister portRegister = globalModule.getConnectorPortRegister();
        Config config = globalModule.getGlobalConfig();
        InternalLogProvider logProvider = globalModule.getLogService().getInternalLogProvider();
        RoutingTableTTLProvider ttlProvider = RoutingTableTTLProvider.ttlFromConfig(config);
        return new SingleAddressRoutingTableProvider(
                portRegister, RoutingOption.ROUTE_WRITE_AND_READ, config, logProvider, ttlProvider);
    }

    public abstract TopologyInfoService createTopologyInfoService(DatabaseContextProvider<?> databaseContextProvider);

    public abstract RoutingService createRoutingService(
            DatabaseContextProvider<?> databaseContextProvider, ClientRoutingDomainChecker clientRoutingDomainChecker);

    public static <T> T tryResolveOrCreate(
            Class<T> clazz, DependencyResolver dependencies, Supplier<T> newInstanceMethod) {
        return dependencies.containsDependency(clazz) ? dependencies.resolveDependency(clazz) : newInstanceMethod.get();
    }

    public static IdContextFactory createIdContextFactory(GlobalModule globalModule) {
        return tryResolveOrCreate(
                IdContextFactory.class,
                globalModule.getExternalDependencyResolver(),
                () -> IdContextFactoryProvider.getInstance().buildIdContextFactory(globalModule));
    }

    public abstract ProcedureConfig getProcedureConfig(Config config);
}
