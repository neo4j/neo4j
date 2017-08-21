/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.IsolationLevel;
import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * Component used by {@link KernelStatement} to acquire explicit (also known as "pessimistic") and
 * {@link #optimistic() optimistic} locks.
 */
public interface StatementLocks extends AutoCloseable
{
    Supplier<LockTracer> DEFAULT_LOCK_TRACER_SUPPLIER = () -> LockTracer.NONE;

    // ====[ Explicit locking ]====

    /**
     * Explicitly acquire an exclusive lock of the given resource type, on the given resources.
     * <p>
     * These locks are always taken, and held until explicitly released, or until the transaction ends.
     *
     * @param type The type of resource to lock on.
     * @param resourceId The resources to lock.
     */
    void explicitAcquireExclusive( ResourceType type, long... resourceId );

    /**
     * Release the exclusive lock that was explicitly taken on the given resource, of the given type.
     *
     * @param type The type of resource that was locked.
     * @param resourceId The resources to unlock.
     */
    void explicitReleaseExclusive( ResourceType type, long resourceId );

    /**
     * Explicitly acquire an shared lock of the given resource type, on the given resources.
     * <p>
     * These locks are always taken, and held until explicitly released, or until the transaction ends.
     *
     * @param type The type of resource to lock on.
     * @param resourceId The resources to lock.
     */
    void explicitAcquireShared( ResourceType type, long... resourceId );

    /**
     * Release the shared lock that was explicitly taken on the given resource, of the given type.
     *
     * @param type The type of resource that was locked.
     * @param resourceId The resources to unlock.
     */
    void explicitReleaseShared( ResourceType type, long resourceId );

    // ====[ Optimistic constraint locks ]====

    /**
     * Get {@link Locks.Client} responsible for optimistic locks. Such locks could potentially be grabbed later at
     * commit time.
     *
     * @return the locks client to serve optimistic locks.
     */
    Locks.Client optimistic();

    /**
     * Acquire an <strong>optimistic</strong> (may be deferred) exclusive lock on the index entry given by the specified
     * resource.
     */
    void uniquenessConstraintEntryAcquireExclusive( long resource );

    /**
     * Release the optimistic exclusive lock that was acquired on the index entry given by the specified resource.
     */
    void uniquenessConstraintEntryReleaseExclusive( long resource );

    /**
     * Acquire an <strong>optimistic</strong> (may be deferred) shared lock on teh index entry given by the specified
     * resource.
     */
    void uniquenessConstraintEntryAcquireShared( long resource );

    /**
     * Release the optimistic shared lock that was acquired on the index entry given by the specified resource.
     */
    void uniquenessConstraintEntryReleaseShared( long resource );

    // ====[ Optimistic locks for schema reading and modifying ]====

    /**
     * Acquire an <strong>optimistic</strong> (may be deferred) exclusive lock on the given schema resource with the
     * given type.
     */
    void schemaModifyAcquireExclusive( ResourceType type, long resource );

    /**
     * Acquire an <strong>optimistic</strong> (may be deferred) shared lock on the given schema resource with the given
     * type.
     */
    void schemaModifyAcquireShared( ResourceType type, long resource );

    /**
     * Prepare the underlying {@link Locks.Client client}(s) for commit. This will grab all locks that have
     * previously been taken {@link #optimistic() optimistically}.
     */
    void prepareForCommit();

    /**
     * Stop the underlying {@link Locks.Client client}(s).
     */
    void stop();

    /**
     * Close the underlying {@link Locks.Client client}(s).
     */
    @Override
    void close();

    /**
     * List the locks held by this transaction.
     * <p>
     * This method is invoked by concurrent threads in order to inspect the lock state in this transaction.
     *
     * @return the locks held by this transaction.
     */
    Stream<? extends ActiveLock> activeLocks();

    /**
     * Get the current number of active locks.
     * <p>
     * Note that the value returned by this method might differ from the number of locks returned by
     * {@link #activeLocks()}, since they would introspect the lock state at different points in time.
     *
     * @return the number of active locks in this transaction.
     */
    long activeLockCount();

    /**
     * @see org.neo4j.kernel.impl.coreapi.InternalTransaction#setIsolationLevel(IsolationLevel)
     */
    void setIsolationLevel( IsolationLevel isolationLevel );

    /**
     * Set the supplier of the tracers used to trace every lock acquire and release.
     *
     * @param lockTracerSupplier The supplier of lock tracers.
     */
    void setLockTracerSupplier( Supplier<LockTracer> lockTracerSupplier );

    /**
     * An id that uniquely identifies the current lock session at this point in time.
     * @return The current lock session id used by this instance of {@link StatementLocks}.
     */
    int getLockSessionId();
}
