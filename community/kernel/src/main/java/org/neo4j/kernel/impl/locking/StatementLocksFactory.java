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

import org.neo4j.kernel.configuration.Config;

/**
 * Factory to create {@link StatementLocks} instances.
 */
public interface StatementLocksFactory
{
    /**
     * Initialize this factory with the given {@code locks} and {@code config}. Callers should ensure this method
     * is called once during database startup.
     *
     * @param locks the locks to use.
     * @param config the database config that can contain settings interesting for factory implementations.
     */
    void initialize( Locks locks, Config config );

    /**
     * Create new {@link StatementLocks} instance.
     *
     * @return new statement locks.
     */
    StatementLocks newInstance();
}
