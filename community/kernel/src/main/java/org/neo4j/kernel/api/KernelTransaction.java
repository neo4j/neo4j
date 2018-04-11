/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.KernelImpl;

/**
 * Represents a transaction of changes to the underlying graph.
 * Actual changes are made in the {@linkplain #acquireStatement() statements} acquired from this transaction.
 * Changes made within a transaction are visible to all operations within it.
 *
 * The reason for the separation between transactions and statements is isolation levels. While Neo4j is read-committed
 * isolation, a read can potentially involve multiple operations (think of a cypher statement). Within that read, or
 * statement if you will, the isolation level should be repeatable read, not read committed.
 *
 * Clearly separating between the concept of a transaction and the concept of a statement allows us to cater to this
 * type of isolation requirements.
 *
 * This class has a 1-1 relationship with{@link org.neo4j.kernel.impl.transaction.state.TransactionRecordState}, please see its'
 * javadoc for details.
 *
 * Typical usage:
 * <pre>
 * try ( KernelTransaction transaction = kernel.newTransaction() )
 * {
 *      try ( Statement statement = transaction.acquireStatement() )
 *      {
 *          ...
 *      }
 *      ...
 *      transaction.success();
 * }
 * catch ( SomeException e )
 * {
 *      ...
 * }
 * catch ( SomeOtherExceptionException e )
 * {
 *      ...
 * }
 * </pre>
 *
 * Typical usage of failure if failure isn't controlled with exceptions:
 * <pre>
 * try ( KernelTransaction transaction = kernel.newTransaction() )
 * {
 *      ...
 *      if ( ... some condition )
 *      {
 *          transaction.failure();
 *      }
 *
 *      transaction.success();
 * }
 * </pre>
 */
public interface KernelTransaction extends Transaction, AssertOpen
{
    interface CloseListener
    {
        /**
         * @param txId On success, the actual id of the current transaction if writes have been performed, 0 otherwise.
         * On rollback, always -1.
         */
        void notify( long txId );
    }

    /**
     * Acquires a new {@link Statement} for this transaction which allows for reading and writing data from and
     * to the underlying database. After the group of reads and writes have been performed the statement
     * must be {@link Statement#close() released}.
     * @return a {@link Statement} with access to underlying database.
     */
    Statement acquireStatement();

    /**
     * @return the security context this transaction is currently executing in.
     */
    SecurityContext securityContext();

    /**
     * @return the subject executing this transaction.
     */
    AuthSubject subject();

    /**
     * @return The timestamp of the last transaction that was committed to the store when this transaction started.
     */
    long lastTransactionTimestampWhenStarted();

    /**
     * @return The id of the last transaction that was committed to the store when this transaction started.
     */
    long lastTransactionIdWhenStarted();

    /**
     * @return start time of this transaction, i.e. basically {@link System#currentTimeMillis()} when user called
     * {@link org.neo4j.internal.kernel.api.Session#beginTransaction(Type)}.
     */
    long startTime();

    /**
     * Timeout for transaction in milliseconds.
     * @return transaction timeout in milliseconds.
     */
    long timeout();

    /**
     * Register a {@link CloseListener} to be invoked after commit, but before transaction events "after" hooks
     * are invoked.
     * @param listener {@link CloseListener} to get these notifications.
     */
    void registerCloseListener( CloseListener listener );

    /**
     * Kernel transaction type
     *
     * Implicit if created internally in the database
     * Explicit if created by the end user
     *
     * @return the transaction type: implicit or explicit
     */
    Type transactionType();

    /**
     * Return transaction id that assigned during transaction commit process.
     * @see org.neo4j.kernel.impl.api.TransactionCommitProcess
     * @return transaction id.
     * @throws IllegalStateException if transaction id is not assigned yet
     */
    long getTransactionId();

    /**
     * Return transaction commit time (in millis) that assigned during transaction commit process.
     * @see org.neo4j.kernel.impl.api.TransactionCommitProcess
     * @return transaction commit time
     * @throws IllegalStateException if commit time is not assigned yet
     */
    long getCommitTime();

    Revertable overrideWith( SecurityContext context );

    ClockContext clocks();

    NodeCursor nodeCursor();

    RelationshipScanCursor relationshipCursor();

    PropertyCursor propertyCursor();

    @FunctionalInterface
    interface Revertable extends AutoCloseable
    {
        @Override
        void close();
    }
}
