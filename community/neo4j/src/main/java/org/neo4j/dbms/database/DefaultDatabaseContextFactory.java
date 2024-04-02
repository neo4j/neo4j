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
package org.neo4j.dbms.database;

import java.util.Map;
import java.util.Optional;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.cypher.internal.javacompat.CommunityCypherEngineProvider;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.GlobalAvailabilityGuardController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.pagecache.CommunityVersionStorageFactory;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;

public class DefaultDatabaseContextFactory
        extends AbstractDatabaseContextFactory<StandaloneDatabaseContext, Optional<?>> {
    private final DatabaseTransactionStats.Factory transactionStatsFactory;
    private final DatabaseIndexStats.Factory indexStatsFactory;
    private final DeviceMapper deviceMapper;
    private final IOControllerService controllerService;
    private final CommitProcessFactory commitProcessFactory;
    private final DefaultDatabaseContextFactoryComponents components;
    private final ServerIdentity serverIdentity;

    public DefaultDatabaseContextFactory(
            GlobalModule globalModule,
            ServerIdentity serverIdentity,
            DatabaseTransactionStats.Factory transactionStatsFactory,
            DatabaseIndexStats.Factory indexStatsFactory,
            IdContextFactory idContextFactory,
            DeviceMapper deviceMapper,
            IOControllerService controllerService,
            CommitProcessFactory commitProcessFactory,
            DefaultDatabaseContextFactoryComponents components) {
        super(globalModule, idContextFactory);
        this.serverIdentity = serverIdentity;
        this.transactionStatsFactory = transactionStatsFactory;
        this.indexStatsFactory = indexStatsFactory;
        this.deviceMapper = deviceMapper;
        this.controllerService = controllerService;
        this.commitProcessFactory = commitProcessFactory;
        this.components = components;
    }

    @Override
    public StandaloneDatabaseContext create(NamedDatabaseId namedDatabaseId, Optional<?> ignored) {
        return new Creator(namedDatabaseId).context();
    }

    private class Creator {
        private final Database kernelDatabase;
        private final StandaloneDatabaseContext context;

        private Creator(NamedDatabaseId namedDatabaseId) {
            var databaseConfig = new DatabaseConfig(Map.of(), globalModule.getGlobalConfig());
            var contextFactory = createContextFactory(databaseConfig, namedDatabaseId);
            var creationContext = new ModularDatabaseCreationContext(
                    HostedOnMode.SINGLE,
                    serverIdentity,
                    namedDatabaseId,
                    globalModule,
                    globalModule.getGlobalDependencies(),
                    contextFactory,
                    deviceMapper,
                    new CommunityVersionStorageFactory(),
                    databaseConfig,
                    globalModule.getGlobalMonitors(),
                    LeaseService.NO_LEASES,
                    () -> DatabaseCreationContext.selectStorageEngine(
                            globalModule.getFileSystem(),
                            globalModule.getNeo4jLayout(),
                            databaseConfig,
                            namedDatabaseId),
                    new StandardConstraintSemantics(),
                    new CommunityCypherEngineProvider(),
                    transactionStatsFactory.create(),
                    indexStatsFactory.create(),
                    ModularDatabaseCreationContext.defaultFileWatcherFilter(),
                    AccessCapabilityFactory.configDependent(),
                    ExternalIdReuseConditionProvider.NONE,
                    idContextFactory.createIdContext(namedDatabaseId, contextFactory, databaseConfig, true),
                    commitProcessFactory,
                    createTokenHolderProvider(this::kernel),
                    new GlobalAvailabilityGuardController(globalModule.getGlobalAvailabilityGuard()),
                    components.readOnlyDatabases(),
                    controllerService,
                    new DatabaseTracers(globalModule.getTracers(), namedDatabaseId));
            kernelDatabase = new Database(creationContext);
            context = new StandaloneDatabaseContext(kernelDatabase);
        }

        private StandaloneDatabaseContext context() {
            return context;
        }

        private Kernel kernel() {
            return kernelDatabase.getDependencyResolver().resolveDependency(Kernel.class);
        }
    }
}
