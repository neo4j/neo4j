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

import java.util.Collection;
import java.util.Collections;
import org.neo4j.graphdb.event.TransactionEventListener;

public interface GlobalTransactionEventListeners {
    /**
     * Registers {@code listener} as a listener for transaction events which
     * are generated from different places in the lifecycle of each
     * transaction.
     *
     * @param databaseName name of the database to listener transactions
     * @param listener     the listener to receive events about different states
     *                     in transaction life cycles.
     */
    void registerTransactionEventListener(String databaseName, TransactionEventListener<?> listener);

    /**
     * Unregisters {@code listener} from the list of transaction event listeners.
     * If {@code handler} hasn't been registered with
     * {@link #registerTransactionEventListener(String, TransactionEventListener)} prior
     * to calling this method an {@link IllegalStateException} will be thrown.
     *
     * @param databaseName name of the database to listener transactions
     * @param listener     the listener to receive events about different states
     *                     in transaction life cycles.
     * @throws IllegalStateException if {@code listener} wasn't registered prior
     *                               to calling this method.
     */
    void unregisterTransactionEventListener(String databaseName, TransactionEventListener<?> listener);

    Collection<TransactionEventListener<?>> getDatabaseTransactionEventListeners(String databaseName);

    GlobalTransactionEventListeners NULL = new GlobalTransactionEventListeners() {
        @Override
        public void registerTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {}

        @Override
        public void unregisterTransactionEventListener(String databaseName, TransactionEventListener<?> listener) {}

        @Override
        public Collection<TransactionEventListener<?>> getDatabaseTransactionEventListeners(String databaseName) {
            return Collections.emptyList();
        }
    };
}
