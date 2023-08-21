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
package org.neo4j.kernel.impl.index.schema;

import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class CompositeTokenScanValueIteratorTest {
    @Inject
    private RandomSupport random;

    @Test
    void mustHandleEmptyListOfIterators() {
        // given
        List<PrimitiveLongResourceIterator> iterators = emptyList();

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, false);

        // then
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void mustHandleEmptyIterator() {
        // given
        List<PrimitiveLongResourceIterator> iterators = singletonList(iterator(0));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, false);

        // then
        assertFalse(iterator.hasNext());
    }

    @Test
    void mustHandleMultipleEmptyIterators() {
        // given
        List<PrimitiveLongResourceIterator> iterators = asMutableList(iterator(0), iterator(1), iterator(2));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, false);

        // then
        assertFalse(iterator.hasNext());
    }

    /* ALL = FALSE */
    @Test
    void mustReportAllFromSingleIterator() {
        // given
        long[] expected = {0L, 1L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = Collections.singletonList(iterator(0, expected));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, false);

        // then
        assertArrayEquals(expected, PrimitiveLongCollections.asArray(iterator));
    }

    @Test
    void mustReportAllFromNonOverlappingMultipleIterators() {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter = {0L, 2L, Long.MAX_VALUE};
        long[] secondIter = {1L, 3L};
        long[] expected = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator(closeCounter::incrementAndGet, 0, firstIter),
                iterator(closeCounter::incrementAndGet, 1, secondIter));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, false);

        // then
        assertArrayEquals(expected, PrimitiveLongCollections.asArray(iterator));

        // when
        iterator.close();

        // then
        assertEquals(2, closeCounter.get(), "expected close count");
    }

    @Test
    void mustReportUniqueValuesFromOverlappingIterators() {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter = {0L, 2L, Long.MAX_VALUE};
        long[] secondIter = {1L, 3L};
        long[] thirdIter = {0L, 3L};
        long[] expected = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator(closeCounter::incrementAndGet, 0, firstIter),
                iterator(closeCounter::incrementAndGet, 1, secondIter),
                iterator(closeCounter::incrementAndGet, 2, thirdIter));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, false);

        // then
        assertArrayEquals(expected, PrimitiveLongCollections.asArray(iterator));

        // when
        iterator.close();

        // then
        assertEquals(3, closeCounter.get(), "expected close count");
    }

    @Test
    void mustReportUniqueValuesFromOverlappingIteratorsWithOneEmpty() {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter = {0L, 2L, Long.MAX_VALUE};
        long[] secondIter = {1L, 3L};
        long[] thirdIter = {0L, 3L};
        long[] fourthIter = {
            /* Empty */
        };
        long[] expected = {0L, 1L, 2L, 3L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator(closeCounter::incrementAndGet, 0, firstIter),
                iterator(closeCounter::incrementAndGet, 1, secondIter),
                iterator(closeCounter::incrementAndGet, 2, thirdIter),
                iterator(closeCounter::incrementAndGet, 3, fourthIter));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, false);

        // then
        assertArrayEquals(expected, PrimitiveLongCollections.asArray(iterator));

        // when
        iterator.close();

        // then
        assertEquals(4, closeCounter.get(), "expected close count");
    }

    /* ALL = TRUE */
    @Test
    void mustOnlyReportValuesReportedByAll() {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter = {0L, Long.MAX_VALUE};
        long[] secondIter = {0L, 1L, Long.MAX_VALUE};
        long[] thirdIter = {0L, 1L, 2L, Long.MAX_VALUE};
        long[] expected = {0L, Long.MAX_VALUE};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator(closeCounter::incrementAndGet, 0, firstIter),
                iterator(closeCounter::incrementAndGet, 1, secondIter),
                iterator(closeCounter::incrementAndGet, 2, thirdIter));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, true);

        // then
        assertArrayEquals(expected, PrimitiveLongCollections.asArray(iterator));

        // when
        iterator.close();

        // then
        assertEquals(3, closeCounter.get(), "expected close count");
    }

    @Test
    void mustOnlyReportValuesReportedByAllWithOneEmpty() {
        // given
        AtomicInteger closeCounter = new AtomicInteger();
        long[] firstIter = {0L, Long.MAX_VALUE};
        long[] secondIter = {0L, 1L, Long.MAX_VALUE};
        long[] thirdIter = {0L, 1L, 2L, Long.MAX_VALUE};
        long[] fourthIter = {
            /* Empty */
        };
        long[] expected = {};
        List<PrimitiveLongResourceIterator> iterators = asMutableList(
                iterator(closeCounter::incrementAndGet, 0, firstIter),
                iterator(closeCounter::incrementAndGet, 1, secondIter),
                iterator(closeCounter::incrementAndGet, 2, thirdIter),
                iterator(closeCounter::incrementAndGet, 3, fourthIter));

        // when
        CompositeTokenScanValueIterator iterator = new CompositeTokenScanValueIterator(iterators, true);

        // then
        assertArrayEquals(expected, PrimitiveLongCollections.asArray(iterator));

        // when
        iterator.close();

        // then
        assertEquals(4, closeCounter.get(), "expected close count");
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void mustReportCorrectValuesRandomized(boolean trueForAll) {
        // given a couple of iterators, one per tokenId
        var numIterators = random.nextInt(2, 10);
        List<long[]> data = new ArrayList<>();
        List<PrimitiveLongResourceIterator> iterators = new ArrayList<>();
        for (var tokenId = 0; tokenId < numIterators; tokenId++) {
            var ids = randomIds(1_000, 4);
            data.add(ids);
            iterators.add(iterator(tokenId, ids));
        }

        // when
        var composite = new CompositeTokenScanValueIterator(iterators, trueForAll);

        // then
        assertThat(PrimitiveLongCollections.asArray(composite)).isEqualTo(calculateExpectedIds(data, trueForAll));
    }

    private long[] calculateExpectedIds(List<long[]> data, boolean trueForAll) {
        var size = data.stream()
                .mapToInt(ids -> toIntExact(ids[ids.length - 1] + 1))
                .max()
                .getAsInt();
        var counts = new int[size];
        data.forEach(ids -> {
            for (var id : ids) {
                counts[toIntExact(id)]++;
            }
        });
        var matchingIds = LongLists.mutable.empty();
        int filter = trueForAll ? data.size() : 1;
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] >= filter) {
                matchingIds.add(i);
            }
        }
        return matchingIds.toArray();
    }

    private long[] randomIds(int numEntries, int maxIdStep) {
        var ids = new long[numEntries];
        for (int i = 0, nextId = random.nextInt(maxIdStep);
                i < numEntries;
                i++, nextId += random.nextInt(1, maxIdStep)) {
            ids[i] = nextId;
        }
        return ids;
    }

    @SafeVarargs
    private static <T> List<T> asMutableList(T... objects) {
        return new ArrayList<>(Arrays.asList(objects));
    }

    private static PrimitiveLongResourceIterator iterator(int tokenId, long... ids) {
        return iterator(Resource.EMPTY, tokenId, ids);
    }

    private static TokenScanValueIterator iterator(Resource resource, int tokenId, long... ids) {
        return new TokenScanValueIterator() {
            private int index = -1;

            @Override
            public int tokenId() {
                return tokenId;
            }

            @Override
            public long next() {
                return ids[++index];
            }

            @Override
            public boolean hasNext() {
                return index + 1 < ids.length;
            }

            @Override
            public void close() {
                resource.close();
            }
        };
    }
}
