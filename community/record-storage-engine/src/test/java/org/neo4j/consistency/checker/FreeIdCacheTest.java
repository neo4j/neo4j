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
package org.neo4j.consistency.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class FreeIdCacheTest {
    @Inject
    private RandomSupport random;

    private static PrimitiveLongResourceIterator iterator(long min, long max, long[] allElements) {
        long[] filtered = Arrays.stream(allElements)
                .filter(l -> l >= min && l <= Math.max(min + 1, max))
                .toArray();
        return PrimitiveLongResourceCollections.iterator(null, filtered);
    }

    @Test
    void shouldFindFreeIdsBelowLimit() throws IOException {
        // given
        long[] free = new long[] {2, 4, 5, 7, 9};
        long[] nonFree = new long[] {1, 3, 6, 8, 10};
        // when
        FreeIdCache freeIdCache = new FreeIdCache(withFreeIds(free), 10);
        freeIdCache.initialize();
        // then
        for (int i = 0; i < free.length; i++) {
            assertThat(freeIdCache.isIdFree(free[i])).isTrue();
            assertThat(freeIdCache.isIdFree(nonFree[i])).isFalse();
        }
    }

    @Test
    void shouldFindFreeIdsAboveLimit() throws IOException {
        // given
        MutableLongSet free = LongSets.mutable.empty();
        MutableLongSet nonFree = LongSets.mutable.empty();
        for (int i = 0; i < 1000; i++) {
            free.add(random.nextLong());
            nonFree.add(random.nextLong());
        }
        nonFree.removeAll(free);
        // when
        FreeIdCache freeIdCache = new FreeIdCache(withFreeIds(free.toArray()), 10);
        freeIdCache.initialize();
        // then
        free.forEach(id -> assertThat(freeIdCache.isIdFree(id)).isTrue());
        nonFree.forEach(id -> assertThat(freeIdCache.isIdFree(id)).isFalse());
    }

    @Test
    void testBloomFilter() {
        // Given
        FreeIdCache.FreeIdsBloomFilter filter = new FreeIdCache.FreeIdsBloomFilter(100, 4);
        List<Long> values = List.of(1L, 10L, 1000L, 10000000L, 1000000000000L, 1000000000000000L);

        // When
        values.forEach(filter::add);
        // Then
        for (long value : values) {
            assertThat(filter.idMayBeFree(value - 1)).isFalse();
            assertThat(filter.idMayBeFree(value)).isTrue();
            assertThat(filter.idMayBeFree(value + 1)).isFalse();
        }
    }

    @Test
    void testBloomFilterRandomNumbers() {
        // Given
        FreeIdCache.FreeIdsBloomFilter filter = new FreeIdCache.FreeIdsBloomFilter(100, 4);
        List<Long> values = new ArrayList<>();
        // When
        for (int i = 0; i < 10; i++) {
            long value = random.nextLong();
            values.add(value);
            filter.add(value);
        }
        // Then
        for (long value : values) {
            assertThat(filter.idMayBeFree(value)).isTrue();
        }
    }

    private IdGenerator withFreeIds(long... ids) throws IOException {
        Arrays.sort(ids);
        IdGenerator mock = mock(IdGenerator.class);
        when(mock.getHighId()).thenReturn(Long.MAX_VALUE);
        when(mock.notUsedIdsIterator()).thenReturn(PrimitiveLongResourceCollections.iterator(null, ids));
        when(mock.notUsedIdsIterator(anyLong(), anyLong()))
                .thenAnswer(inv -> iterator(inv.getArgument(0), inv.getArgument(1), ids));
        return mock;
    }
}
