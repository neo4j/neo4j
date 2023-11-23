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
package org.neo4j.fabric.bootstrap;

import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.SystemNanoClock;

public class CommonQueryRouterBoostrap {

    private final ServiceBootstrapper serviceBootstrapper;
    protected final Dependencies dependencies;
    private final DatabaseContextProvider<? extends DatabaseContext> databaseProvider;

    public CommonQueryRouterBoostrap(
            LifeSupport lifeSupport,
            Dependencies dependencies,
            DatabaseContextProvider<? extends DatabaseContext> databaseProvider) {
        this.dependencies = dependencies;
        this.serviceBootstrapper = new ServiceBootstrapper(lifeSupport, dependencies);
        this.databaseProvider = databaseProvider;
    }

    protected void bootstrapCommonServices(DatabaseManagementService databaseManagementService, LogService logService) {
        if (!dependencies.containsDependency(LocalGraphTransactionIdTracker.class)) {
            var monitors = resolve(Monitors.class);
            var serverConfig = resolve(Config.class);
            var systemNanoClock = resolve(SystemNanoClock.class);
            var logProvider = logService.getInternalLogProvider();
            var transactionIdTracker =
                    new TransactionIdTracker(databaseManagementService, monitors, systemNanoClock, logProvider);
            var databaseIdRepository = databaseProvider.databaseIdRepository();
            register(
                    new LocalGraphTransactionIdTracker(transactionIdTracker, databaseIdRepository, serverConfig),
                    LocalGraphTransactionIdTracker.class);
        }
    }

    protected <T> T register(T dependency, Class<T> dependencyType) {
        return serviceBootstrapper.registerService(dependency, dependencyType);
    }

    protected <T> T resolve(Class<T> type) {
        return dependencies.resolveDependency(type);
    }

    protected <T extends Lifecycle> void registerWithLifecycle(T instance) {
        serviceBootstrapper.lifeSupport.add(instance);
    }

    private record ServiceBootstrapper(LifeSupport lifeSupport, Dependencies dependencies) {

        <T> T registerService(T dependency, Class<T> dependencyType) {
            dependencies.satisfyDependency(dependency);

            if (LifecycleAdapter.class.isAssignableFrom(dependencyType)) {
                lifeSupport.add((LifecycleAdapter) dependency);
            }

            return dependencies.resolveDependency(dependencyType);
        }
    }
}
