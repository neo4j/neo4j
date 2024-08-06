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
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;

@FunctionalInterface
public interface SystemDatabaseProvider {
    class SystemDatabaseUnavailableException extends RuntimeException {}

    class SystemDatabasePanickedException extends SystemDatabaseUnavailableException {}

    GraphDatabaseAPI database() throws SystemDatabaseUnavailableException;

    default void execute(Consumer<Transaction> consumer) throws SystemDatabaseUnavailableException {
        var facade = database();
        if (!facade.isAvailable(1000)) {
            throw new SystemDatabaseUnavailableException();
        }
        try (var tx = facade.beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
    }

    default <T> T query(Function<Transaction, T> function) throws SystemDatabaseUnavailableException {
        return query(database(), function, true).orElseThrow();
    }

    default <T> Optional<T> queryIfAvailable(Function<Transaction, T> function) {
        return query(database(), function, false);
    }

    default <T> Optional<T> dependency(Class<T> type) throws SystemDatabaseUnavailableException {
        return dependency(database().getDependencyResolver(), type);
    }

    static <T> Optional<T> dependency(DependencyResolver dependencies, Class<T> type)
            throws SystemDatabaseUnavailableException {
        if (dependencies.containsDependency(type)) {
            return Optional.of(dependencies.resolveDependency(type));
        }
        return Optional.empty();
    }

    private static <T> Optional<T> query(
            GraphDatabaseAPI facade, Function<Transaction, T> function, boolean waitForAvailablity)
            throws SystemDatabaseUnavailableException {
        if (waitForAvailablity) {
            if (!facade.isAvailable(1000)) {
                if (dependency(facade.getDependencyResolver(), DatabaseHealth.class)
                        .map(DatabaseHealth::hasNoPanic)
                        .orElse(true)) {
                    throw new SystemDatabaseUnavailableException();
                }
                throw new SystemDatabasePanickedException();
            }
        } else if (!facade.isAvailable()) {
            return Optional.empty();
        }
        try (var tx = facade.beginTx()) {
            var result = function.apply(tx);
            tx.commit();
            return Optional.of(result);
        }
    }
}
