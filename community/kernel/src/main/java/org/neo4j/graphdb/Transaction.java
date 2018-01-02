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
package org.neo4j.graphdb;

/**
 * A programmatically handled transaction.
 * <p>
 * <em>All database operations that access the graph, indexes, or the schema must be performed in a transaction.</em>
 * <p>
 * If you attempt to access the graph outside of a transaction, those operations will throw
 * {@link NotInTransactionException}.
 * <p>
 * Transactions are bound to the thread in which they were created. Transactions can either be handled
 * programmatically, through this interface, or by a container through the Java Transaction API (JTA). The
 * Transaction interface makes handling programmatic transactions easier than using JTA programmatically.
 * Here's the idiomatic use of programmatic transactions in Neo4j starting from java 7:
 * 
 * <pre>
 * <code>
 * try ( Transaction tx = graphDb.beginTx() )
 * {
 *     // operations on the graph
 *     // ...
 * 
 *     tx.success();
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
 * as resource. At the end of the block, we invoke the {@link #success() tx.success()}
 * method to indicate that the transaction is successful. As we exit the block,
 * the transaction will automatically be closed where {@link #close() tx.close()}
 * will be called and commit the transaction if the internal state indicates success
 * or else mark it for rollback.
 * <p>
 * If an exception is raised in the try-block, {@link #success()} will never be
 * invoked and the internal state of the transaction object will cause
 * {@link #close()} to roll back the transaction. This is very important:
 * unless {@link #success()} is invoked, the transaction will fail upon
 * {@link #close()}. A transaction can be explicitly marked for rollback by
 * invoking the {@link #failure()} method.
 * <p>
 * Read operations inside of a transaction will also read uncommitted data from
 * the same transaction.
 * <p>
 * 
 * Here's the idiomatic use of programmatic transactions in Neo4j on java 6 or earlier:
 * 
 * <pre>
 * <code>
 * Transaction tx = graphDb.beginTx();
 * try
 * {
 *     // operations on the graph
 *     // ...
 * 
 *     tx.success();
 * }
 * finally
 * {
 *     tx.close();
 * }
 * </code>
 * </pre>
 * <p>
 * All {@link ResourceIterable ResourceIterables} that where returned from operations executed inside a transaction
 * will be automatically closed when the transaction is committed or rolled back.
 * Note however, that the {@link ResourceIterator} should be {@link ResourceIterator#close() closed} as soon as
 * possible if you don't intend to exhaust the iterator.
 */
public interface Transaction extends AutoCloseable
{
    /**
     * Marks this transaction as terminated, which means that it will be, much like in the case of failure,
     * unconditionally rolled back when {@link #close()} is called. Once this method has been invoked, it doesn't matter
     * if {@link #success()} is invoked afterwards -- the transaction will still be rolled back.
     *
     * Additionally, terminating a transaction causes all subsequent operations carried out within that
     * transaction to throw a {@link TransactionTerminatedException} in the owning thread.
     *
     * Note that, unlike the other transaction operations, this method can be called from threads other than
     * the owning thread of the transaction. When this method is called from a different thread,
     * it signals the owning thread to terminate the transaction and returns immediately.
     *
     * Calling {@link #terminate()} on an already closed transaction has no effect.
     */
    void terminate();

    /**
     * Marks this transaction as failed, which means that it will
     * unconditionally be rolled back when {@link #close()} is called. Once
     * this method has been invoked, it doesn't matter if
     * {@link #success()} is invoked afterwards -- the transaction will still be
     * rolled back.
     */
    void failure();

    /**
     * Marks this transaction as successful, which means that it will be
     * committed upon invocation of {@link #close()} unless {@link #failure()}
     * has or will be invoked before then.
     */
    void success();

    /**
     * Commits or marks this transaction for rollback, depending on whether
     * {@link #success()} or {@link #failure()} has been previously invoked.
     * 
     * All {@link ResourceIterable ResourceIterables} that where returned from operations executed inside this
     * transaction will be automatically closed by this method.
     * 
     * Preferably this method will not be used, instead a {@link Transaction} should participate in a
     * try-with-resource statement so that {@link #close()} is automatically called instead.
     * 
     * Invoking {@link #close()} (which is unnecessary when in try-with-resource statement) or this method
     * has the exact same effect.
     * 
     * @deprecated due to implementing {@link AutoCloseable}, where {@link #close()} is called automatically
     * when used in try-with-resource statements.
     */
    @Deprecated
    void finish();
    
    /**
     * Commits or marks this transaction for rollback, depending on whether
     * {@link #success()} or {@link #failure()} has been previously invoked.
     * 
     * All {@link ResourceIterable ResourceIterables} that where returned from operations executed inside this
     * transaction will be automatically closed by this method.
     * 
     * This method comes from {@link AutoCloseable} so that a {@link Transaction} can participate
     * in try-with-resource statements. It will not throw any declared exception.
     * 
     * Invoking this method (which is unnecessary when in try-with-resource statement) or {@link #finish()}
     * has the exact same effect.
     */
    @Override
    void close();
    
    /**
     * Acquires a write lock for {@code entity} for this transaction.
     * The lock (returned from this method) can be released manually, but
     * if not it's released automatically when the transaction finishes.
     * @param entity the entity to acquire a lock for. If another transaction
     * currently holds a write lock to that entity this call will wait until
     * it's released.
     * 
     * @return a {@link Lock} which optionally can be used to release this
     * lock earlier than when the transaction finishes. If not released
     * (with {@link Lock#release()} it's going to be released with the
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
