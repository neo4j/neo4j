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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checker.EntityBasedMemoryLimiter;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheckIntegrationTest;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.IndexEntryUpdate;

import static org.neo4j.consistency.checking.cache.CacheSlots.CACHE_LINE_SIZE_BYTES;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

class LimitedFullCheckIT extends FullCheckIntegrationTest
{
    @Override
    protected EntityBasedMemoryLimiter.Factory memoryLimit()
    {
        // Make it so that it will have to do the checking in a couple of node id ranges
        return ( pageCacheMemory, highNodeId ) -> new EntityBasedMemoryLimiter( pageCacheMemory, 0, pageCacheMemory + highNodeId * CACHE_LINE_SIZE_BYTES / 3,
                CACHE_LINE_SIZE_BYTES, highNodeId, 1 );
    }

    @Test
    void shouldFindDuplicatesInUniqueIndexEvenWhenInDifferentRanges() throws ConsistencyCheckIncompleteException, IndexEntryConflictException, IOException
    {
        // given
        Iterator<IndexDescriptor> indexRuleIterator = getValueIndexDescriptors();

        // Create 2 extra nodes to guarantee that the node id of our duplicate is not in the same range as the original entry.
        createOneNode();
        createOneNode();
        // Create a node so the duplicate in the index refers to a valid node
        // (IndexChecker only reports the duplicate if it refers to a node id lower than highId)
        long nodeId = createOneNode();
        while ( indexRuleIterator.hasNext() )
        {
            IndexDescriptor indexRule = indexRuleIterator.next();
                // Don't close this accessor. It will be done when shutting down db.
                IndexAccessor accessor = fixture.indexAccessorLookup().apply( indexRule );

                try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
                {
                    // There is already another node (created in generateInitialData()) that has this value
                    updater.process( IndexEntryUpdate.add( nodeId, indexRule, values( indexRule ) ) );
                }
                accessor.force( NULL );
        }

        // when
        ConsistencySummaryStatistics stats = check();

        // then
        on( stats ).verify( RecordType.NODE, 2 ) // the duplicate in the 2 unique indexes
                .verify( RecordType.INDEX, 6 ) // the index entries pointing to node that should not be in index (3 BTREE and 3 RANGE)
                .andThatsAllFolks();
    }
}
