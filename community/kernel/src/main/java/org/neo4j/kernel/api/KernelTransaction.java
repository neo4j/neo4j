/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.ClockContext;

/**
 * Extends the outwards-facing {@link org.neo4j.internal.kernel.api.Transaction} with additional functionality
 * that is used inside the kernel (and in some other places, ahum). Please do not rely on this class unless you
 * have to.
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
     *
     * @return a {@link Statement} with access to underlying database.
     */
    Statement acquireStatement();

    /**
     * @return the security context this transaction is currently executing in.
     * @throws NotInTransactionException if the transaction is closed.
     */
    SecurityContext securityContext();

    /**
     * @return the subject executing this transaction, or {@link AuthSubject#ANONYMOUS} if the transaction is closed.
     */
    AuthSubject subjectOrAnonymous();

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

    /**
     * Temporarily override this transaction's SecurityContext. The override should be reverted using
     * the returned {@link Revertable}.
     *
     * @param context the temporary SecurityContext.
     * @return {@link Revertable} which reverts to the original SecurityContext.
     */
    Revertable overrideWith( SecurityContext context );

    /**
     * Clocks associated with this transaction.
     */
    ClockContext clocks();

    /**
     * USE WITH CAUTION:
     * The internal node cursor instance used to serve kernel API calls. If some kernel API call
     * is made while this cursor is used, it might get corrupted and return wrong results.
     */
    NodeCursor ambientNodeCursor();

    /**
     * USE WITH CAUTION:
     * The internal relationship scan cursor instance used to serve kernel API calls. If some kernel
     * API call is made while this cursor is used, it might get corrupted and return wrong results.
     */
    RelationshipScanCursor ambientRelationshipCursor();

    /**
     * USE WITH CAUTION:
     * The internal property cursor instance used to serve kernel API calls. If some kernel
     * API call is made while this cursor is used, it might get corrupted and return wrong results.
     */
    PropertyCursor ambientPropertyCursor();

    @FunctionalInterface
    interface Revertable extends AutoCloseable
    {
        @Override
        void close();
    }
}
