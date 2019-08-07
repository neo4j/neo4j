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
package org.neo4j.graphdb;

import org.neo4j.annotations.api.PublicApi;

/**
 * A programmatically handled transaction.
 * <p>
 * <em>All database operations that access the graph, indexes, or the schema must be performed in a transaction.</em>
 * <p>
 * If you attempt to access the graph outside of a transaction, those operations will throw
 * {@link NotInTransactionException}.
 * <p>
 * Here's the idiomatic use of programmatic transactions in Neo4j:
 *
 * <pre>
 * <code>
 * try ( Transaction tx = graphDb.beginTx() )
 * {
 *     // operations on the graph
 *     // ...
 *
 *     tx.commit();
 * }
 * </code>
 * </pre>
 *
 * <p>
 * Let's walk through this example line by line. First we retrieve a Transaction
 * object by invoking the {@link GraphDatabaseService#beginTx()} factory method.
 * This creates a new transaction which has internal state to keep
 * track of whether the current transaction is successful. Then we wrap all
 * operations that modify the graph in a try-finally block with the transaction
 * as resource. At the end of the block, we invoke the {@link #commit() tx.commit()}
 * method to commit that the transaction.
 * <p>
 * If an exception is raised in the try-block, {@link #commit()} will never be
 * invoked and the transaction will be roll backed. This is very important:
 * unless {@link #commit()} is invoked, the transaction will fail upon
 * {@link #close()}. A transaction can be explicitly rolled back by
 * invoking the {@link #rollback()} method.
 * <p>
 * Read operations inside of a transaction will also read uncommitted data from
 * the same transaction.
 * <p>
 * <p>
 * All {@link ResourceIterable ResourceIterables} that where returned from operations executed inside a transaction
 * will be automatically closed when the transaction is committed or rolled back.
 * Note however, that the {@link ResourceIterator} should be {@link ResourceIterator#close() closed} as soon as
 * possible if you don't intend to exhaust the iterator.
 */
@PublicApi
public interface Transaction extends AutoCloseable
{
    /**
     * Marks this transaction as terminated, which means that it will be, much like in the case of failure,
     * unconditionally rolled back when {@link #close()} is called. Once this method has been invoked, it doesn't matter
     * if {@link #commit()} ()} is invoked afterwards -- the transaction will still be rolled back.
     *
     * Additionally, terminating a transaction causes all subsequent operations carried out within that
     * transaction to throw a {@link TransactionTerminatedException} in the owning thread.
     *
     * Note that, unlike the other transaction operations, this method can be called from threads other than
     * the owning thread of the transaction. When this method is called from a different thread,
     * it signals the owning thread to terminate the transaction and returns immediately.
     *
     * Calling this method on an already closed transaction has no effect.
     */
    void terminate();

    /**
     * Commit and close current transaction.
     * <p>
     * When {@code commit()} is completed, all resources are released and no more changes are possible in this transaction.
     */
    void commit();

    /**
     * Roll back and close current transaction.
     * When {@code rollback()} is completed, all resources are released and no more changes are possible in this transaction
     */
    void rollback();

    /**
     * Close transaction. If {@link #commit()} or {@link #rollback()} have been called this does nothing.
     * If none of them are called, the transaction will be rolled back.
     *
     * <p>All {@link ResourceIterable ResourceIterables} that where returned from operations executed inside this
     * transaction will be automatically closed by this method in they were not closed before.
     *
     * <p>This method comes from {@link AutoCloseable} so that a {@link Transaction} can participate
     * in try-with-resource statements.
     */
    @Override
    void close();

    /**
     * Acquires a write lock for {@code entity} for this transaction.
     * The lock (returned from this method) can be released manually, but
     * if not it's released automatically when the transaction finishes.
     *
     * @param entity the entity to acquire a lock for. If another transaction
     * currently holds a write lock to that entity this call will wait until
     * it's released.
     *
     * @return a {@link Lock} which optionally can be used to release this
     * lock earlier than when the transaction finishes. If not released
     * (with {@link Lock#release()} it's going to be released when the
     * transaction finishes.
     */
    Lock acquireWriteLock( PropertyContainer entity );

    /**
     * Acquires a read lock for {@code entity} for this transaction.
     * The lock (returned from this method) can be released manually, but
     * if not it's released automatically when the transaction finishes.
     * @param entity the entity to acquire a lock for. If another transaction
     * currently hold a write lock to that entity this call will wait until
     * it's released.
     *
     * @return a {@link Lock} which optionally can be used to release this
     * lock earlier than when the transaction finishes. If not released
     * (with {@link Lock#release()} it's going to be released with the
     * transaction finishes.
     */
    Lock acquireReadLock( PropertyContainer entity );
}
