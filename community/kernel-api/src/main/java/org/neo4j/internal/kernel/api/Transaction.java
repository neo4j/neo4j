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
package org.neo4j.internal.kernel.api;

import java.util.Optional;

import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * A transaction with the graph database.
 *
 * Access to the graph is performed via sub-interfaces like {@link org.neo4j.internal.kernel.api.Read}.
 * Changes made within a transaction are immediately visible to all operations within it, but are only
 * visible to other transactions after the successful commit of the transaction.
 *
 * Typical usage:
 * <pre>
 * try ( Transaction transaction = session.beginTransaction() )
 * {
 *      ...
 *      transaction.success();
 * }
 * catch ( SomeException e )
 * {
 *      ...
 * }
 * </pre>
 *
 * Typical usage of failure if failure isn't controlled with exceptions:
 * <pre>
 * try ( Transaction transaction = session.beginTransaction() )
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
public interface Transaction extends AutoCloseable
{
    enum Type
    {
        implicit,
        explicit
    }

    /**
     * The store id of a rolled back transaction.
     */
    long ROLLBACK = -1;

    /**
     * The store id of a read-only transaction.
     */
    long READ_ONLY = 0;

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
     * @return The Read operations of the graph. The returned instance targets the active transaction state layer.
     */
    Read dataRead();

    /**
     * @return The Read operations of the graph. The returned instance targets the stable transaction state layer.
     */
    Read stableDataRead();

    /**
     * Stabilize the active transaction state. This moves all changes up until this point to the stable
     * transaction state layer. Any further changes will be added to the (now empty) active transaction state.
     */
    void markAsStable();

    /**
     * @return The Write operations of the graph. The returned instance writes to the active transaction state layer.
     * @throws InvalidTransactionTypeKernelException when transaction cannot be upgraded to a write transaction. This
     * can happen when there have been schema modifications.
     */
    Write dataWrite() throws InvalidTransactionTypeKernelException;

    /**
     * @return The explicit index read operations of the graph.
     */
    ExplicitIndexRead indexRead();

    /**
     * @return The explicit index write operations of the graph.
     */
    ExplicitIndexWrite indexWrite() throws InvalidTransactionTypeKernelException;

    /**
     * @return Token read operations
     */
    TokenRead tokenRead();

    /**
     * @return Token read operations
     */
    TokenWrite tokenWrite();

    /**
     * @return Token read and write operations
     */
    Token token();

    /**
     * @return The schema index read operations of the graph, used for finding indexes.
     */
    SchemaRead schemaRead();

    /**
     * @return The schema index write operations of the graph, used for creating and dropping indexes and constraints.
     */
    SchemaWrite schemaWrite() throws InvalidTransactionTypeKernelException;

    /**
     * @return The lock operations of the graph.
     */
    Locks locks();

    /**
     * @return The cursor factory
     */
    CursorFactory cursors();

    /**
     * @return Returns procedure operations
     */
    Procedures procedures();

    /**
     * @return statistics about the execution
     */
    ExecutionStatistics executionStatistics();

    /**
     * Closes this transaction, committing its changes if {@link #success()} has been called and neither
     * {@link #failure()} nor {@link #markForTermination(Status)} has been called.
     * Otherwise its changes will be rolled back.
     *
     * @return id of the committed transaction or {@link #ROLLBACK} if transaction was rolled back or
     * {@link #READ_ONLY} if transaction was read-only.
     */
    long closeTransaction() throws TransactionFailureException;

    /**
     * Closes this transaction, committing its changes if {@link #success()} has been called and neither
     * {@link #failure()} nor {@link #markForTermination(Status)} has been called.
     * Otherwise its changes will be rolled back.
     */
    @Override
    default void close() throws TransactionFailureException
    {
        closeTransaction();
    }

    /**
     * @return {@code true} if the transaction is still open, i.e. if {@link #close()} hasn't been called yet.
     */
    boolean isOpen();

    /**
     * @return {@link Status} if {@link #markForTermination(Status)} has been invoked, otherwise empty optional.
     */
    Optional<Status> getReasonIfTerminated();

    /**
     * @return true if transaction was terminated, otherwise false
     */
    boolean isTerminated();

    /**
     * Marks this transaction for termination, such that it cannot commit successfully and will try to be
     * terminated by having other methods throw a specific termination exception, as to sooner reach the assumed
     * point where {@link #close()} will be invoked.
     */
    void markForTermination( Status reason );
}
