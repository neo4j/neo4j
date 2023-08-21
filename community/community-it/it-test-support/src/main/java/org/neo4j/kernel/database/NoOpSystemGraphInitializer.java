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
package org.neo4j.kernel.database;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.SystemGraphInitializer;

public final class NoOpSystemGraphInitializer {
    private NoOpSystemGraphInitializer() { // no-op
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link NamedDatabaseId} from system database.
     * <p>
     * Note: this will also modify {@code config} by setting
     * {@link GraphDatabaseInternalSettings#fallback_to_latest_runtime_version} to {@code true}.
     * @param config Used for default database name
     * @return Dependencies that can set as external dependencies in DatabaseManagementServiceBuilder
     */
    public static Dependencies noOpSystemGraphInitializer(Config config) {
        return noOpSystemGraphInitializer(new Dependencies(), config);
    }

    /**
     * Disable SystemGraphInitializer to avoid interfering with tests or changing backups. Injects {@link TestDatabaseIdRepository} as it will no longer
     * be possible to read {@link NamedDatabaseId} from system database.
     * <p>
     * Note: this will also modify {@code config} by setting
     * {@link GraphDatabaseInternalSettings#fallback_to_latest_runtime_version} to {@code true}.
     * @param dependencies to include in returned {@link DependencyResolver}
     * @param config Used for default database name
     * @return Dependencies that can set as external dependencies in DatabaseManagementServiceBuilder
     */
    public static Dependencies noOpSystemGraphInitializer(DependencyResolver dependencies, Config config) {
        return noOpSystemGraphInitializer(new Dependencies(dependencies), config);
    }

    private static Dependencies noOpSystemGraphInitializer(Dependencies dependencies, Config config) {
        config.set(GraphDatabaseInternalSettings.fallback_to_latest_runtime_version, true);
        dependencies.satisfyDependencies(SystemGraphInitializer.NO_OP, new TestDatabaseIdRepository(config));
        return dependencies;
    }
}
