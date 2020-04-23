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
package org.neo4j.memory;

import org.apache.commons.lang3.StringUtils;

public interface ScopedMemoryPool extends MemoryPool, AutoCloseable
{
    /**
     * Memory pool group
     */
    MemoryGroup group();

    /**
     * Database name for non global pools, otherwise empty
     * @return name of the database this pool belongs or empty if global
     */
    default String databaseName()
    {
        return StringUtils.EMPTY;
    }

    /**
     * Close and de-register specific memory pool
     */
    void close();

    /**
     * @return thread safe memory tracker for this pool
     */
    MemoryTracker getPoolMemoryTracker();
}
