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
package org.neo4j.kernel.internal.event;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.neo4j.graphdb.event.TransactionEventListener;

public class DefaultGlobalTransactionEventListeners implements GlobalTransactionEventListeners {
    private final ConcurrentHashMap<String, List<TransactionEventListener<?>>> globalTransactionEventListeners =
            new ConcurrentHashMap<>();

    @Override
    public void registerTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {
        requireNonNull(databaseName, "Database name is required.");
        requireNonNull(listener, "Transaction event listener is required.");
        globalTransactionEventListeners.compute(databaseName, (s, transactionEventListeners) -> {
            List<TransactionEventListener<?>> listeners =
                    transactionEventListeners != null ? transactionEventListeners : new CopyOnWriteArrayList<>();
            if (listeners.contains(listener)) {
                return listeners;
            }
            listeners.add(listener);
            return listeners;
        });
    }

    @Override
    public void unregisterTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {
        requireNonNull(databaseName);
        requireNonNull(listener);
        globalTransactionEventListeners.compute(databaseName, (s, transactionEventListeners) -> {
            if (transactionEventListeners == null || !transactionEventListeners.remove(listener)) {
                throw new IllegalStateException(format(
                        "Transaction event listener `%s` is not registered as listener for database `%s`.",
                        listener, databaseName));
            }
            if (transactionEventListeners.isEmpty()) {
                return null;
            }
            return transactionEventListeners;
        });
    }

    @Override
    public Collection<TransactionEventListener<?>> getDatabaseTransactionEventListeners(String databaseName) {
        var listeners = globalTransactionEventListeners.getOrDefault(databaseName, null);
        return listeners == null ? Collections.emptyList() : new ArrayList<>(listeners);
    }
}
