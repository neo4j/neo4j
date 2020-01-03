/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.event;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;

/**
 * An event handler interface for transaction events. Once it has been
 * registered at a {@link DatabaseManagementService} instance it will receive events
 * about what has happened in each transaction which is about to be committed
 * and has any data that is accessible via {@link TransactionData}.
 * Handlers won't get notified about transactions which hasn't performed any
 * write operation or won't be committed (either if
 * {@link Transaction#commit()} hasn't been called or the transaction has been
 * rolled back, {@link Transaction#rollback()}.
 * <p>
 * Right before a transaction is about to be committed the
 * {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)} method is called with the entire diff
 * of modifications made in the transaction. At this point the transaction is
 * still running so changes can still be made. However there's no guarantee that
 * other handlers will see such changes since the order in which handlers are
 * executed is undefined. This method can also throw an exception and will, in
 * such a case, prevent the transaction from being committed.
 * <p>
 * If {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)} is successfully executed the
 * transaction will be committed and the
 * {@link #afterCommit(TransactionData, Object, GraphDatabaseService)} method will be called with the
 * same transaction data as well as the object returned from
 * {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)}. This assumes that all other handlers
 * (if more were registered) also executed
 * {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)} successfully.
 * <p>
 * If {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)} isn't executed successfully, but
 * instead throws an exception the transaction won't be committed and a
 * {@link TransactionFailureException} will (eventually) be thrown from
 * {@link Transaction#close()}. All handlers which at this point have had its
 * {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)} method executed successfully will
 * receive a call to {@link #afterRollback(TransactionData, Object, GraphDatabaseService)}.
 *
 * @param <T> The type of a state object that the transaction handler can use to
 *            pass information from the {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)}
 *            event dispatch method to the
 *            {@link #afterCommit(TransactionData, Object, GraphDatabaseService)} or
 *            {@link #afterRollback(TransactionData, Object, GraphDatabaseService)} method, depending
 *            on whether the transaction succeeded or failed.
 */
@PublicApi
public interface TransactionEventListener<T>
{
    /**
     * Invoked when a transaction that has changes accessible via {@link TransactionData}
     * is about to be committed.
     *
     * If this method throws an exception the transaction will be rolled back
     * and a {@link TransactionFailureException} will be thrown from
     * {@link Transaction#close()}.
     *
     * The transaction is still open when this method is invoked, making it
     * possible to perform mutating operations in this method. This is however
     * highly discouraged since changes made in this method are not guaranteed to be
     * visible by this or other {@link TransactionEventListener}s.
     *
     * @param data the changes that will be committed in this transaction.
     * @param transaction ongoing transaction
     * @param databaseService underlying database service
     * @return a state object (or <code>null</code>) that will be passed on to
     *         {@link #afterCommit(TransactionData, Object, GraphDatabaseService)} or
     *         {@link #afterRollback(TransactionData, Object, GraphDatabaseService)} of this object.
     * @throws Exception to indicate that the transaction should be rolled back.
     */
    T beforeCommit( TransactionData data, Transaction transaction, GraphDatabaseService databaseService ) throws Exception;

    /**
     * Invoked after the transaction has been committed successfully.
     * Any {@link TransactionData} being passed in to this method is guaranteed
     * to first have been called with {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)}.
     * At the point of calling this method the transaction have been closed
     * and so accessing data outside that of what the {@link TransactionData}
     * can provide will require a new transaction to be opened.
     *  @param data the changes that were committed in this transaction.
     * @param state the object returned by
     *            {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)}.
     * @param databaseService underlying database service
     */
    void afterCommit( TransactionData data, T state, GraphDatabaseService databaseService );

    /**
     * Invoked after the transaction has been rolled back if committing the
     * transaction failed for some reason.
     * Any {@link TransactionData} being passed in to this method is guaranteed
     * to first have been called with {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)}.
     * At the point of calling this method the transaction have been closed
     * and so accessing data outside that of what the {@link TransactionData}
     * can provide will require a new transaction to be opened.
     *  @param data the changes that were attempted to be committed in this transaction.
     * @param state the object returned by {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)}.
     * If this handler failed when executing {@link #beforeCommit(TransactionData, Transaction, GraphDatabaseService)} this
     * {@code state} will be {@code null}.
     * @param databaseService underlying database service
     */
    void afterRollback( TransactionData data, T state, GraphDatabaseService databaseService );
}
