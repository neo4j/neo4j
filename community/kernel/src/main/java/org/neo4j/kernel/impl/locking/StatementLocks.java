/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.KernelStatement;

/**
 * Component used by {@link KernelStatement} to acquire {@link #implicit() implicit} and {@link #explicit() explicit}
 * locks.
 */
public interface StatementLocks extends AutoCloseable
{
    /**
     * Get {@link Locks.Client} responsible for explicit locks. Such locks are explicitly grabbed by the user via
     * {@link Transaction#acquireReadLock(PropertyContainer)} and
     * {@link Transaction#acquireWriteLock(PropertyContainer)}.
     *
     * @return the locks client to serve explicit locks.
     */
    Locks.Client explicit();

    /**
     * Get {@link Locks.Client} responsible for implicit locks. Such locks are grabbed by the database itself to
     * provide consistency guarantees.
     *
     * @return the locks client to serve implicit locks.
     */
    Locks.Client implicit();

    /**
     * Prepare the underlying {@link Locks.Client client}(s) for commit.
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
}
