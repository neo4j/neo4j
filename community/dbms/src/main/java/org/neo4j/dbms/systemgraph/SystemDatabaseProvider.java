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
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

@FunctionalInterface
public interface SystemDatabaseProvider {
    class SystemDatabaseUnavailableException extends RuntimeException {}

    GraphDatabaseAPI database() throws SystemDatabaseUnavailableException;

    default <T> T execute(BiFunction<GraphDatabaseAPI, Transaction, T> function)
            throws SystemDatabaseUnavailableException {
        var facade = database();
        if (!facade.isAvailable(1000)) {
            throw new SystemDatabaseUnavailableException();
        }
        try (var tx = facade.beginTx()) {
            return function.apply(facade, tx);
        }
    }

    default <T> Optional<T> dependency(Class<T> type) throws SystemDatabaseUnavailableException {
        var dependencies = database().getDependencyResolver();
        if (dependencies.containsDependency(type)) {
            return Optional.of(dependencies.resolveDependency(type));
        }
        return Optional.empty();
    }
}
