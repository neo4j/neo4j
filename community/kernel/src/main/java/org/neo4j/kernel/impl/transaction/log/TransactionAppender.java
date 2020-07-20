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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * Writes batches of transactions, each containing groups of commands to a log that is guaranteed to be recoverable,
 * i.e. consistently readable, in the event of failure.
 */
public interface TransactionAppender
{
    /**
     * Appends a batch of transactions to a log, effectively committing the transactions.
     * After this method have returned the returned transaction id should be visible in
     * {@link TransactionIdStore#getLastCommittedTransactionId()}.
     * <p>
     * Any failure happening inside this method will cause a {@link DatabaseHealth#panic(Throwable) kernel panic}.
     * Callers must make sure that successfully appended
     * transactions exiting this method are {@link Commitment#publishAsClosed(PageCursorTracer)}.
     *
     * @param batch transactions to append to the log. These transaction instances provide both input arguments
     * as well as a place to provide output data, namely {@link TransactionToApply#commitment(Commitment, long)} and
     * {@link TransactionToApply#transactionId()}.
     * @param logAppendEvent A trace event for the given log append operation.
     * @return last committed transaction in this batch. The appended (i.e. committed) transactions
     * will have had their {@link TransactionToApply#commitment(Commitment, long)} available and caller is expected to
     * {@link Commitment#publishAsClosed(PageCursorTracer)} mark them as applied} after they have been applied to storage.
     * Note that {@link Commitment commitments} must be {@link Commitment#publishAsCommitted(PageCursorTracer)}  marked as committed}
     * by this method.
     * @throws IOException if there was a problem appending the transaction. See method javadoc body for
     * how to handle exceptions in general thrown from this method.
     */
    long append( TransactionToApply batch, LogAppendEvent logAppendEvent ) throws IOException;
}
