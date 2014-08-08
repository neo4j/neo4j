/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

/**
 * Keeps a latest transaction id. There's one counter for {@code committing transaction id} and one for
 * {@code applied transaction id}. The committing transaction id is for writing into a log before making
 * the changes to be made. After that the application of those transactions might be asynchronous and
 * completion of those are marked using {@link #transactionClosed(long)}.
 */
public interface TransactionIdStore
{
    // Tx id counting starting from this value (this value means no transaction in the log)
    public static final long BASE_TX_ID = 1;

    /**
     * @return the next transaction id for a committing transaction. The transaction id is incremented
     * with each call.
     */
    long nextCommittingTransactionId();

    long getLastCommittingTransactionId();

    /**
     * Used by recovery. Perhaps this shouldn't be exposed like this?
     */
    void setLastCommittingAndClosedTransactionId( long transactionId );

    /**
     * Signals that a transaction with a given transaction id has been applied. Calls to this method
     * may come in out-of-transaction-id order.
     * @param transactionId the applied transaction id.
     */
    void transactionClosed( long transactionId );

    /**
     * Should be called in a place where no more committing transaction ids are returned, so that
     * applied transactions can catch up.
     *
     * @return {@code true} if the latest applied transaction (without any lower transaction id gaps)
     * is the same as the highest returned {@code committing transaction id}.
     */
    boolean closedTransactionIdIsOnParWithCommittingTransactionId();

    /**
     * Forces the transaction id to persistent storage.
     */
    void flush();
}
