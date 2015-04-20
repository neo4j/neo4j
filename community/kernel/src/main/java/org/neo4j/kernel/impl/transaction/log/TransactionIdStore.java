/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

/**
 * Keeps a latest transaction id. There's one counter for {@code committed transaction id} and one for
 * {@code closed transaction id}. The committed transaction id is for writing into a log before making
 * the changes to be made. After that the application of those transactions might be asynchronous and
 * completion of those are marked using {@link #transactionClosed(long)}.
 *
 * A transaction ID passes through a {@link TransactionIdStore} like this:
 * <ol>
 *   <li>{@link #nextCommittingTransactionId()} is called and an id is returned to a committer.
 *   At this point that id isn't visible from any getter.</li>
 *   <li>{@link #transactionCommitted(long, long)} is called with this id after the fact that the transaction
 *   has been committed, i.e. written forcefully to a log. After this call the id may be visible from
 *   {@link #getLastCommittedTransactionId()} if all ids before it have also been committed.</li>
 *   <li>{@link #transactionClosed(long)} is called with this id again, this time after all changes the
 *   transaction imposes have been applied to the store. At this point this id is regarded in
 *   {@link #closedTransactionIdIsOnParWithOpenedTransactionId()} as well.
 * </ol>
 */
public interface TransactionIdStore
{
    // Tx id counting starting from this value (this value means no transaction ever committed)
    public static final long BASE_TX_ID = 1;
    public static final long BASE_TX_CHECKSUM = 0;

    /**
     * @return the next transaction id for a committing transaction. The transaction id is incremented
     * with each call. Ids returned from this method will not be visible from {@link #getLastCommittedTransactionId()}
     * until handed to {@link #transactionCommitted(long, long)}.
     */
    long nextCommittingTransactionId();

    /**
     * Signals that a transaction with the given transaction id has been committed (i.e. appended to a log).
     * Calls to this method may come in out-of-transaction-id order. The highest gap-free transaction id
     * seen given to this method will be visible in {@link #getLastCommittedTransactionId()}.
     * @param transactionId the applied transaction id.
     * @param checksum checksum of the transaction.
     */
    void transactionCommitted( long transactionId, long checksum );

    /**
     * @return highest seen gap-free {@link #transactionCommitted(long, long) committed transaction id}.
     */
    long getLastCommittedTransactionId();

    /**
     * Returns transaction information about the last committed transaction, i.e.
     * transaction id as well as checksum.
     */
    long[] getLastCommittedTransaction();

    /**
     * Returns transaction information about transaction where the last upgrade was performed, i.e.
     * transaction id as well as checksum.
     */
    long[] getUpgradeTransaction();

    /**
     * @return highest seen gap-free {@link #transactionClosed(long) closed transaction id}.
     */
    long getLastClosedTransactionId();

    /**
     * Used by recovery, where last committed/closed transaction ids are set.
     * Perhaps this shouldn't be exposed like this?
     *
     * @param transactionId transaction id that will be the last closed/committed id.
     * @param checksum checksum of the transaction.
     */
    void setLastCommittedAndClosedTransactionId( long transactionId, long checksum );

    /**
     * Signals that a transaction with the given transaction id has been fully applied. Calls to this method
     * may come in out-of-transaction-id order.
     * @param transactionId the applied transaction id.
     */
    void transactionClosed( long transactionId );

    /**
     * Should be called in a place where no more committed transaction ids are returned, so that
     * applied transactions can catch up.
     *
     * @return {@code true} if the latest applied transaction (without any lower transaction id gaps)
     * is the same as the highest returned {@code committed transaction id}.
     */
    boolean closedTransactionIdIsOnParWithOpenedTransactionId();

    /**
     * Forces the transaction id counters to persistent storage.
     */
    void flush();
}
