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
package org.neo4j.dbms.database;

import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.cypher.internal.javacompat.CommunityCypherEngineProvider;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.GlobalAvailabilityGuardController;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.pagecache.CommunityVersionStorageFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class DefaultDatabaseContextFactory extends AbstractDatabaseContextFactory<StandaloneDatabaseContext> {
    private final DatabaseTransactionStats.Factory transactionStatsFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final DefaultDatabaseContextFactoryComponents components;

    public DefaultDatabaseContextFactory(
            GlobalModule globalModule,
            DatabaseTransactionStats.Factory transactionStatsFactory,
            IdContextFactory idContextFactory,
            CommitProcessFactory commitProcessFactory,
            DefaultDatabaseContextFactoryComponents components) {
        super(globalModule, idContextFactory);
        this.transactionStatsFactory = transactionStatsFactory;
        this.commitProcessFactory = commitProcessFactory;
        this.components = components;
    }

    @Override
    public StandaloneDatabaseContext create(NamedDatabaseId namedDatabaseId, DatabaseOptions databaseOptions) {
        return new Creator(namedDatabaseId, databaseOptions).context();
    }

    private class Creator {
        private final Database kernelDatabase;
        private final StandaloneDatabaseContext context;

        private Creator(NamedDatabaseId namedDatabaseId, DatabaseOptions databaseOptions) {
            var databaseConfig = new DatabaseConfig(databaseOptions.settings(), globalModule.getGlobalConfig());
            var contextFactory = createContextFactory(databaseConfig, namedDatabaseId);
            StorageEngineFactory storageEngineFactory = DatabaseCreationContext.selectStorageEngine(
                    globalModule.getFileSystem(), globalModule.getNeo4jLayout(), databaseConfig, namedDatabaseId);
            var creationContext = new ModularDatabaseCreationContext(
                    databaseOptions.mode(),
                    namedDatabaseId,
                    globalModule,
                    globalModule.getGlobalDependencies(),
                    contextFactory,
                    new CommunityVersionStorageFactory(),
                    databaseConfig,
                    globalModule.getGlobalMonitors(),
                    LeaseService.NO_LEASES,
                    storageEngineFactory,
                    new StandardConstraintSemantics(),
                    new CommunityCypherEngineProvider(),
                    transactionStatsFactory.create(),
                    ModularDatabaseCreationContext.defaultFileWatcherFilter(),
                    AccessCapabilityFactory.configDependent(),
                    ExternalIdReuseConditionProvider.NONE,
                    storageEngineFactory.createLocks(globalModule.getGlobalConfig(), globalModule.getGlobalClock()),
                    idContextFactory.createIdContext(namedDatabaseId, contextFactory),
                    commitProcessFactory,
                    createTokenHolderProvider(this::kernel),
                    new GlobalAvailabilityGuardController(globalModule.getGlobalAvailabilityGuard()),
                    components.readOnlyDatabases());
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
