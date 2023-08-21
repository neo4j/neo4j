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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.api.KernelTransaction;

/**
 * Used as a marker interface for internal transaction event listener, which will get called for all transactions containing changes,
 * even transactions with only token changes (something which the public {@link TransactionEventListener} doesn't get).
 */
public interface InternalTransactionEventListener<T> extends TransactionEventListener<T> {
    /**
     * Difference is that the provided transaction is a {@link KernelTransaction}
     * Note that only one will be invoked
     * @see TransactionEventListener#beforeCommit(TransactionData, Transaction, GraphDatabaseService)
     */
    default T beforeCommit(TransactionData data, KernelTransaction transaction, GraphDatabaseService databaseService)
            throws Exception {
        return beforeCommit(data, transaction.internalTransaction(), databaseService);
    }

    class Adapter<T> extends TransactionEventListenerAdapter<T> implements InternalTransactionEventListener<T> {}
}
