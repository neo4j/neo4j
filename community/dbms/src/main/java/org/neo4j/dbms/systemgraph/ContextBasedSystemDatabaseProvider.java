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
package org.neo4j.dbms.systemgraph;

import java.util.Optional;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class ContextBasedSystemDatabaseProvider implements SystemDatabaseProvider {
    private final DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider;

    public ContextBasedSystemDatabaseProvider(
            DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider) {
        this.databaseContextProvider = databaseContextProvider;
    }

    @Override
    public GraphDatabaseAPI database() throws SystemDatabaseUnavailableException {
        return databaseContext().databaseFacade();
    }

    @Override
    public <T> Optional<T> dependency(Class<T> type) throws SystemDatabaseUnavailableException {
        var dependencies = databaseContext().dependencies();
        if (dependencies.containsDependency(type)) {
            return Optional.of(dependencies.resolveDependency(type));
        }
        return Optional.empty();
    }

    private DatabaseContext databaseContext() {
        return databaseContextProvider
                .getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
                .orElseThrow(SystemDatabaseUnavailableException::new);
    }
}
