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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.state.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;

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
 * This class has a 1-1 relationship with{@link org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState}, please see its'
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
public interface KernelTransaction extends AutoCloseable
{
    /**
     * Acquires a new {@link Statement} for this transaction which allows for reading and writing data from and
     * to the underlying database. After the group of reads and writes have been performed the statement
     * must be {@link Statement#close() released}.
     * @return a {@link Statement} with access to underlying database.
     */
    Statement acquireStatement();

    /**
     * Marks this transaction as successful. When this transaction later gets {@link #close() closed}
     * its changes, if any, will be committed. If this method hasn't been called or if {@link #failure()}
     * has been called then any changes in this transaction will be rolled back as part of {@link #close() closing}.
     */
    void success();

    /**
     * Marks this transaction as failed. No amount of calls to {@link #success()} will clear this flag.
     * When {@link #close() closing} this transaction any changes will be rolled back.
     */
    void failure();

    /**
     * Closes this transaction, committing its changes iff {@link #success()} has been called and
     * {@link #failure()} has NOT been called. Otherwise its changes will be rolled back.
     */
    @Override
    public void close() throws TransactionFailureException;

    /*
     * TODO 2.2-future TransactionRecordState should not be visible anywhere. The problem is that there is
     * TODO 2.2-future some inversion happening at some places where KernelTransaction should delegate down to
     * TODO 2.2-future TRS instead. This must be fixed. This sentiment applies to all accessor methods below.
     */
    TransactionRecordState getTransactionRecordState();

    LegacyIndexTransactionState getLegacyIndexTransactionState();

    boolean isOpen();

    boolean shouldBeTerminated();

    void markForTermination();
}
