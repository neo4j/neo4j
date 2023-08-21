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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class DatabaseMemoryPoolsIT {
    @Inject
    private MemoryPools memoryPools;

    @Inject
    private GraphDatabaseService db;

    @Test
    void trackDatabaseNativeByteBuffersUsage() {
        var otherGlobalPool = memoryPools.getPools().stream()
                .filter(pool -> MemoryGroup.OTHER == pool.group())
                .findFirst()
                .orElseThrow();

        assertThat(otherGlobalPool.usedNative()).isGreaterThan(0);

        var databasePools = otherGlobalPool.getDatabasePools();
        assertThat(databasePools)
                .hasSize(2)
                .anyMatch(pool -> SYSTEM_DATABASE_NAME.equals(pool.databaseName()))
                .anyMatch(pool -> db.databaseName().equals(pool.databaseName()))
                .allMatch(pool -> pool.usedNative() > 0);
    }
}
