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
package org.neo4j.collection;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.neo4j.collection.PrimitiveLongCollections.mergeToSet;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.PrimitiveLongCollections.AbstractPrimitiveLongBaseIterator;

class PrimitiveLongCollectionsTest {
    @Test
    void singleIterator() {
        LongIterator iterator = PrimitiveLongCollections.single(42);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(42);
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void arrayOfItemsAsIterator() {
        // GIVEN
        long[] items = new long[] {2, 5, 234};

        // WHEN
        LongIterator iterator = PrimitiveLongCollections.iterator(items);

        // THEN
        assertItems(iterator, items);
    }

    @Test
    void reverseArrayOfItemsAsIterator() {
        // GIVEN
        long[] items = new long[] {2, 5, 234};

        // WHEN
        LongIterator iterator = PrimitiveLongCollections.reverseIterator(items);

        // THEN
        assertItems(iterator, 234, 5, 2);
    }

    @Test
    void filter() {
        // GIVEN
        LongIterator items = PrimitiveLongCollections.iterator(1, 2, 3);

        // WHEN
        LongIterator filtered = PrimitiveLongCollections.filter(items, item -> item != 2);

        // THEN
        assertItems(filtered, 1, 3);
    }

    @Test
    void indexOf() {
        // GIVEN
        Supplier<LongIterator> items = () -> PrimitiveLongCollections.iterator(10, 20, 30);

        // THEN
        assertThat(PrimitiveLongCollections.indexOf(items.get(), 55)).isEqualTo(-1);
        assertThat(PrimitiveLongCollections.indexOf(items.get(), 10)).isZero();
        assertThat(PrimitiveLongCollections.indexOf(items.get(), 20)).isOne();
        assertThat(PrimitiveLongCollections.indexOf(items.get(), 30)).isEqualTo(2);
    }

    @Test
    void count() {
        // GIVEN
        LongIterator items = PrimitiveLongCollections.iterator(1, 2, 3);

        // WHEN
        int count = PrimitiveLongCollections.count(items);

        // THEN
        assertThat(count).isEqualTo(3);
    }

    @Test
    void asArray() {
        // GIVEN
        LongIterator items = PrimitiveLongCollections.iterator(1, 2, 3);

        // WHEN
        long[] array = PrimitiveLongCollections.asArray(items);

        // THEN
        assertThat(array).containsExactly(new long[] {1, 2, 3});
    }

    @Test
    void shouldNotContinueToCallNextOnHasNextFalse() {
        // GIVEN
        AtomicLong count = new AtomicLong(2);
        LongIterator iterator = new AbstractPrimitiveLongBaseIterator() {
            @Override
            protected boolean fetchNext() {
                return count.decrementAndGet() >= 0 && next(count.get());
            }
        };

        // WHEN/THEN
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isOne();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isZero();
        assertThat(iterator.hasNext()).isFalse();
        assertThat(iterator.hasNext()).isFalse();
        assertThat(count.get()).isEqualTo(-1L);
    }

    @Test
    void convertJavaCollectionToSetOfPrimitives() {
        List<Long> longs = asList(1L, 4L, 7L);
        LongSet longSet = PrimitiveLongCollections.asSet(longs);
        assertThat(longSet.contains(1L)).isTrue();
        assertThat(longSet.contains(4L)).isTrue();
        assertThat(longSet.contains(7L)).isTrue();
        assertThat(longSet.size()).isEqualTo(3);
    }

    @Test
    void convertPrimitiveSetToJavaSet() {
        LongSet longSet = newSetWith(1L, 3L, 5L);
        Set<Long> longs = PrimitiveLongCollections.toSet(longSet);
        assertThat(longs).contains(1L, 3L, 5L);
    }

    @Test
    void mergeLongIterableToSet() {
        assertThat(mergeToSet(new LongHashSet(), new LongHashSet())).isEqualTo(new LongHashSet());
        assertThat(mergeToSet(newSetWith(1, 2, 3), new LongHashSet())).isEqualTo(newSetWith(1, 2, 3));
        assertThat(mergeToSet(newSetWith(1, 2, 3), newSetWith(1, 2, 3, 4, 5, 6)))
                .isEqualTo(newSetWith(1, 2, 3, 4, 5, 6));
        assertThat(mergeToSet(newSetWith(1, 2, 3), newSetWith(4, 5, 6))).isEqualTo(newSetWith(1, 2, 3, 4, 5, 6));
    }

    private static void assertNoMoreItems(LongIterator iterator) {
        assertThat(iterator.hasNext())
                .as(iterator + " should have no more items")
                .isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    private static void assertNextEquals(long expected, LongIterator iterator) {
        assertThat(iterator.hasNext())
                .as(iterator + " should have had more items")
                .isTrue();
        assertThat(iterator.next()).isEqualTo(expected);
    }

    private static void assertItems(LongIterator iterator, long... expectedItems) {
        for (long expectedItem : expectedItems) {
            assertNextEquals(expectedItem, iterator);
        }
        assertNoMoreItems(iterator);
    }
}
