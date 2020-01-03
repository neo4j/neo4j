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
package org.neo4j.kernel.impl.api;

public interface LeaseClient
{
    /**
     * The ID of an acquired lease.
     *
     * Will be {@link LeaseService#NO_LEASE} before ensureValid() has been called and keeps
     * its value even if the lease becomes invalid.
     */
    int leaseId();

    /**
     * Ensures that a valid lease is held or throws an exception.
     *
     * If no lease is held, then an attempt it made to acquire the lease.
     * If a previously valid lease has become invalid, then no new lease will be acquired.
     *
     * @throws LeaseException if a valid lease isn't held.
     */
    void ensureValid() throws LeaseException;
}
