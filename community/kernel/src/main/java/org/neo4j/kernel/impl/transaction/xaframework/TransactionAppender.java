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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

/**
 * Writing groups of commands, in a way that is guaranteed to be recoverable, i.e. consistently readable,
 * in the event of failure.
 */
public interface TransactionAppender
{
    /**
     * @param transaction transaction representation to append.
     * @return transaction id the appended transaction got.
     * @throws TransactionAppendException if there was a problem appending the transaction.
     * If the append got so far as to
     * {@link TransactionAppendException#hasNewTransactionIdGenerated() generate a new transaction id} the
     * {@link TransactionAppendException exception} is able to
     * {@link TransactionAppendException#newTransactionIdGenerated() return that id} so that the caller
     * can notify services that the particular transaction has failed, and is closed in general.
     */
    long append( TransactionRepresentation transaction ) throws TransactionAppendException;

    /**
     * TODO the fact that this method returns a boolean and may "silently" ignore transactions
     * that have already been applied is an artifact of poor architecture in other places, where
     * we allow multiple threads to append committed transactions simultaneously.
     *   One part of the problem is we should not append the same transaction more than once. That can be
     * circumvented by synchronizing on this appender and checking previously last committed transaction id
     * before applying the supplied transaction.
     *   The other part is applying the transactions to the store, which normally can be done in parallel
     * since higher level entity locks are held, making sure that any two transactions writes contended records
     * in the correct order. When receiving and applying committed transactions there's no such coordination
     * by higher level entity locks, and so some other guard must be put in, allowing only a single transaction
     * at a time to apply anything to the store.
     *
     * The above problems would go away if there were only a single thread applying updates. To keep things
     * pure and nice and good, I think that is what we need to do.
     *
     * Appends a transaction which already has a transaction id assigned to it.
     * @param transaction transaction to write.
     * @return {@code true} if the supplied transaction is the previously last committed
     * transaction {@code +1}. If the supplied transaction has a lower transaction id than that
     * {@code false} is returned.
     * @throws IOException if there was a problem writing the transaction, or if the transaction id
     * of the supplied transaction was {@code <=} last committed transaction id.
     */
    boolean append( CommittedTransactionRepresentation transaction ) throws TransactionAppendException;
    
    /**
     * Closes resources held by this appender.
     */
    void close();
}
