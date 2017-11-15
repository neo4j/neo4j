/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.cache;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

/**
 * A cache for in-flight entries which also tracks the size of the cache.
 */
public interface InFlightCache
{
    /**
     * Enables the cache.
     */
    void enable();

    /**
     * Put item into the cache.
     *
     * @param logIndex the index of the log entry.
     * @param entry the Raft log entry.
     */
    void put( long logIndex, RaftLogEntry entry );

    /**
     * Get item from the cache.
     *
     * @param logIndex the index of the log entry.
     * @return the log entry.
     */
    RaftLogEntry get( long logIndex );

    /**
     * Disposes of a range of elements from the tail of the consecutive cache.
     *
     * @param fromIndex the index to start from (inclusive).
     */
    void truncate( long fromIndex );

    /**
     * Prunes items from the cache.
     *
     * @param upToIndex the last index to prune (inclusive).
     */
    void prune( long upToIndex );

    /**
     * @return the amount of data in the cache.
     */
    long totalBytes();

    /**
     * @return the number of log entries in the cache.
     */
    int elementCount();
}
