/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class RelationshipCountsProcessorTest {

    private static final int ANY = -1;
    private final NodeLabelsCache nodeLabelCache = mock(NodeLabelsCache.class);
    private final CountsUpdater countsUpdater = mock(CountsUpdater.class);

    @Test
    void shouldHandleBigNumberOfLabelsAndRelationshipTypes() {
        /*
         * This test ensures that the RelationshipCountsProcessor does not attempt to allocate a negative amount
         * of memory when trying to get an array to store the relationship counts. This could happen when the labels
         * and relationship types were enough in number to overflow an integer used to hold a product of those values.
         * Here we ask the Processor to do that calculation and ensure that the number passed to the NumberArrayFactory
         * is positive.
         */
        // Given
        /*
         * A large but not impossibly large number of labels and relationship types. These values are the simplest
         * i could find in a reasonable amount of time that would result in an overflow. Given that the calculation
         * involves squaring the labelCount, 22 bits are more than enough for an integer to overflow. However, the
         * actual issue involves adding a product of relTypeCount and some other things, which makes hard to predict
         * which values will make it go negative. These worked. Given that with these values the integer overflows
         * some times over, it certainly works with much smaller numbers, but they don't come out of a nice simple bit
         * shifting.
         */
        int relTypeCount = 1 << 8;
        int labelCount = 1 << 22;
        NumberArrayFactory numberArrayFactory = mock(NumberArrayFactory.class);

        // When
        new RelationshipCountsProcessor(
                nodeLabelCache, labelCount, relTypeCount, countsUpdater, numberArrayFactory, INSTANCE);

        // Then
        verify(numberArrayFactory, times(2))
                .newLongArray(longThat(new IsNonNegativeLong()), anyLong(), any(MemoryTracker.class));
    }

    @Test
    void testRelationshipCountersUpdates() {
        int relationTypes = 2;
        int labels = 3;

        NodeLabelsCache.Client client = mock(NodeLabelsCache.Client.class);
        when(nodeLabelCache.newClient()).thenReturn(client);
        when(nodeLabelCache.get(eq(client), eq(1L))).thenReturn(new int[] {0, 2});
        when(nodeLabelCache.get(eq(client), eq(2L))).thenReturn(new int[] {1});
        when(nodeLabelCache.get(eq(client), eq(3L))).thenReturn(new int[] {1, 2});
        when(nodeLabelCache.get(eq(client), eq(4L))).thenReturn(new int[] {});

        RelationshipCountsProcessor countsProcessor = new RelationshipCountsProcessor(
                nodeLabelCache,
                labels,
                relationTypes,
                countsUpdater,
                NumberArrayFactories.AUTO_WITHOUT_PAGECACHE,
                INSTANCE);

        countsProcessor.process(record(1, 0, 3), StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);
        countsProcessor.process(record(2, 1, 4), StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);

        countsProcessor.done();

        // wildcard counts
        verify(countsUpdater).incrementRelationshipCount(ANY, ANY, ANY, 2L);
        verify(countsUpdater).incrementRelationshipCount(ANY, 0, ANY, 1L);
        verify(countsUpdater).incrementRelationshipCount(ANY, 1, ANY, 1L);

        // start labels counts
        verify(countsUpdater).incrementRelationshipCount(0, 0, ANY, 1L);
        verify(countsUpdater).incrementRelationshipCount(2, 0, ANY, 1L);

        // end labels counts
        verify(countsUpdater).incrementRelationshipCount(ANY, 0, 1, 1L);
        verify(countsUpdater).incrementRelationshipCount(ANY, 0, 2, 1L);
    }

    private static class IsNonNegativeLong implements ArgumentMatcher<Long> {
        @Override
        public boolean matches(Long argument) {
            return argument != null && argument >= 0;
        }
    }

    private static RelationshipRecord record(long startNode, int type, long endNode) {
        RelationshipRecord record = new RelationshipRecord(0);
        record.setInUse(true);
        record.setFirstNode(startNode);
        record.setSecondNode(endNode);
        record.setType(type);
        return record;
    }
}
