/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.checker.full;

import org.neo4j.consistency.checker.NodeBasedMemoryLimiter;
import org.neo4j.consistency.checking.full.FullCheckIntegrationSSTITest;

import static org.neo4j.consistency.checking.cache.CacheSlots.CACHE_LINE_SIZE_BYTES;

class LimitedFullCheckSSTIIT extends FullCheckIntegrationSSTITest
{
    @Override
    protected NodeBasedMemoryLimiter.Factory memoryLimit()
    {
        // Make it so that it will have to do the checking in a couple of node id ranges
        return ( pageCacheMemory, highNodeId ) ->
                new NodeBasedMemoryLimiter( pageCacheMemory, 0, pageCacheMemory + highNodeId * CACHE_LINE_SIZE_BYTES / 3, CACHE_LINE_SIZE_BYTES, highNodeId );
    }
}
