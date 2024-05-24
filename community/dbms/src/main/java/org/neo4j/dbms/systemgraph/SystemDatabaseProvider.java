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
import java.util.function.BiFunction;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class SystemDatabaseProvider {
    public static class SystemDatabaseUnavailableException extends RuntimeException {}

    private final DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider;

    public SystemDatabaseProvider(DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider) {
        this.databaseContextProvider = databaseContextProvider;
    }

    public <T> T execute(BiFunction<GraphDatabaseAPI, Transaction, T> function) {
        var context = databaseContextProvider.getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID);
        if (context.isEmpty()) {
            throw new SystemDatabaseUnavailableException();
        }
        var facade = context.get().databaseFacade();

        if (!facade.isAvailable(1000)) {
            throw new SystemDatabaseUnavailableException();
        }
        try (var tx = facade.beginTx()) {
            return function.apply(facade, tx);
        }
    }

    public <T> Optional<T> dependency(Class<T> type) {
        var context = databaseContextProvider.getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID);
        if (context.isEmpty()) {
            throw new DatabaseManagementException("System Database is not yet started");
        }
        var dependencies = context.get().dependencies();
        if (dependencies.containsDependency(type)) {
            return Optional.of(dependencies.resolveDependency(type));
        }
        return Optional.empty();
    }
}
