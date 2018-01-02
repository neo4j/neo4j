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
package org.neo4j.kernel.impl.locking;

import java.util.stream.Stream;

import org.neo4j.kernel.impl.api.KernelStatement;

/**
 * Component used by {@link KernelStatement} to acquire {@link #pessimistic() pessimistic} and
 * {@link #optimistic() optimistic} locks.
 */
public interface StatementLocks extends AutoCloseable, org.neo4j.internal.kernel.api.Locks
{
    /**
     * Get {@link Locks.Client} responsible for pessimistic locks. Such locks will be grabbed right away.
     *
     * @return the locks client to serve pessimistic locks.
     */
    Locks.Client pessimistic();

    /**
     * Get {@link Locks.Client} responsible for optimistic locks. Such locks could potentially be grabbed later at
     * commit time.
     *
     * @return the locks client to serve optimistic locks.
     */
    Locks.Client optimistic();

    /**
     * Prepare the underlying {@link Locks.Client client}(s) for commit. This will grab all locks that have
     * previously been taken {@link #optimistic() optimistically}, and tell the underlying lock client to enter the
     * <em>prepare</em> state.
     * @param lockTracer lock tracer
     */
    void prepareForCommit( LockTracer lockTracer );

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
     *
     * This method is invoked by concurrent threads in order to inspect the lock state in this transaction.
     *
     * @return the locks held by this transaction.
     */
    Stream<? extends ActiveLock> activeLocks();

    /**
     * Get the current number of active locks.
     *
     * Note that the value returned by this method might differ from the number of locks returned by
     * {@link #activeLocks()}, since they would introspect the lock state at different points in time.
     *
     * @return the number of active locks in this transaction.
     */
    long activeLockCount();
}
