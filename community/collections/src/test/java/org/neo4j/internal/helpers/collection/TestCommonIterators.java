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
package org.neo4j.internal.helpers.collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.assertj.core.util.Sets;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

class TestCommonIterators {
    @Test
    void testFirstElement() {
        Object object = new Object();
        Object object2 = new Object();
        Object defaultValue = new Object();

        // first Iterable
        assertEquals(object, Iterables.first(asList(object, object2)));
        assertEquals(object, Iterables.first(singletonList(object)));
        assertThrows(NoSuchElementException.class, () -> Iterables.first(Collections.emptyList()));

        // first Iterator
        assertEquals(object, Iterators.first(asList(object, object2).iterator()));
        assertEquals(object, Iterators.first(singletonList(object).iterator()));
        assertThrows(NoSuchElementException.class, () -> Iterators.first(Collections.emptyIterator()));

        // firstOrNull Iterable
        assertEquals(object, Iterables.firstOrNull(asList(object, object2)));
        assertEquals(object, Iterables.firstOrNull(singletonList(object)));
        assertNull(Iterables.firstOrNull(Collections.emptyList()));

        // firstOrNull Iterator
        assertEquals(object, Iterators.firstOrNull(asList(object, object2).iterator()));
        assertEquals(object, Iterators.firstOrNull(singletonList(object).iterator()));
        assertNull(Iterators.firstOrNull(Collections.emptyIterator()));

        // firstOrDefault
        assertEquals(object, Iterators.firstOrDefault(asList(object, object2).iterator(), defaultValue));
        assertEquals(object, Iterators.firstOrDefault(singletonList(object).iterator(), defaultValue));
        assertEquals(defaultValue, Iterators.firstOrDefault(Collections.emptyIterator(), defaultValue));
    }

    @Test
    void firstElementClosesResourceIterator() {
        // given
        var closed = new MutableBoolean();
        var iterator = Iterators.resourceIterator(Iterators.iterator("a", "b", "c"), closed::setTrue);

        // when
        var first = Iterators.first(iterator);

        // then
        assertThat(closed.getValue()).isTrue();
        assertThat(first).isEqualTo("a");
    }

    @Test
    void testLastElement() {
        Object object = new Object();
        Object object2 = new Object();

        // last Iterable
        assertEquals(object2, Iterables.last(asList(object, object2)));
        assertEquals(object, Iterables.last(singletonList(object)));
        assertThrows(NoSuchElementException.class, () -> Iterables.last(Collections.emptyList()));

        // last Iterator
        assertEquals(object2, Iterators.last(asList(object, object2).iterator()));
        assertEquals(object, Iterators.last(singletonList(object).iterator()));
        assertThrows(NoSuchElementException.class, () -> Iterators.last(Collections.emptyIterator()));

        // lastOrNull Iterator
        assertEquals(object2, Iterators.lastOrNull(asList(object, object2).iterator()));
        assertEquals(object, Iterators.lastOrNull(singletonList(object).iterator()));
        assertNull(Iterators.lastOrNull(Collections.emptyIterator()));
    }

    @Test
    void lastElementClosesResourceIterator() {
        // given
        var closed = new MutableBoolean();
        var iterator = Iterators.resourceIterator(Iterators.iterator("a", "b", "c"), closed::setTrue);

        // when
        var last = Iterators.last(iterator);

        // then
        assertThat(closed.getValue()).isTrue();
        assertThat(last).isEqualTo("c");
    }

    @Test
    void testSingleElement() {
        Object object = new Object();
        Object object2 = new Object();

        // single Iterable
        assertEquals(object, Iterables.single(singletonList(object)));
        assertThrows(NoSuchElementException.class, () -> Iterables.single(Collections.emptyList()));
        assertThrows(NoSuchElementException.class, () -> Iterables.single(asList(object, object2)));

        // single Iterator
        assertEquals(object, Iterators.single(singletonList(object).iterator()));
        assertThrows(NoSuchElementException.class, () -> Iterators.single(Collections.emptyIterator()));
        assertThrows(
                NoSuchElementException.class,
                () -> Iterators.single(asList(object, object2).iterator()));

        // singleOrNull Iterable
        assertEquals(object, Iterables.singleOrNull(singletonList(object)));
        assertNull(Iterables.singleOrNull(Collections.emptyList()));
        assertThrows(NoSuchElementException.class, () -> Iterables.singleOrNull(asList(object, object2)));

        // singleOrNull Iterator
        assertEquals(object, Iterators.singleOrNull(singletonList(object).iterator()));
        assertNull(Iterators.singleOrNull(Collections.emptyIterator()));
        assertThrows(
                NoSuchElementException.class,
                () -> Iterators.singleOrNull(asList(object, object2).iterator()));
    }

    @Test
    void singleElementClosesResourceIterator() {
        // given
        var closed = new MutableBoolean();
        var iterator = Iterators.resourceIterator(Iterators.iterator("a"), closed::setTrue);

        // when
        var single = Iterators.single(iterator);

        // then
        assertThat(closed.getValue()).isTrue();
        assertThat(single).isEqualTo("a");
    }

    @Test
    void getItemFromEnd() {
        Iterable<Integer> ints = asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertEquals((Integer) 9, Iterators.fromEnd(ints.iterator(), 0));
        assertEquals((Integer) 8, Iterators.fromEnd(ints.iterator(), 1));
        assertEquals((Integer) 7, Iterators.fromEnd(ints.iterator(), 2));
    }

    @Test
    void fromEndClosesResourceIterator() {
        // given
        var closed = new MutableBoolean();
        var iterator = Iterators.resourceIterator(Iterators.iterator("a", "b", "c"), closed::setTrue);

        // when
        var end = Iterators.fromEnd(iterator, 1);

        // then
        assertThat(closed.getValue()).isTrue();
        assertThat(end).isEqualTo("b");
    }

    @Test
    void iteratorsStreamForNull() {
        assertThrows(NullPointerException.class, () -> Iterators.stream(null));
    }

    @Test
    void iteratorsStream() {
        List<Object> list = asList(1, 2, "3", '4', null, "abc", "56789");

        Iterator<Object> iterator = list.iterator();

        assertEquals(list, Iterators.stream(iterator).collect(toList()));
    }

    @Test
    void iteratorsStreamClosesResourceIterator() {
        List<Object> list = asList("a", "b", "c", "def");

        Resource resource = mock(Resource.class);
        ResourceIterator<Object> iterator = Iterators.resourceIterator(list.iterator(), resource);

        try (Stream<Object> stream = Iterators.stream(iterator)) {
            assertEquals(list, stream.collect(toList()));
        }
        verify(resource).close();
    }

    @Test
    void iteratorsStreamCharacteristics() {
        Iterator<Integer> iterator = asList(1, 2, 3).iterator();
        int characteristics = Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.SORTED;

        Stream<Integer> stream = Iterators.stream(iterator, characteristics);

        assertEquals(characteristics, stream.spliterator().characteristics());
    }

    @Test
    void iterablesStreamForNull() {
        assertThrows(NullPointerException.class, () -> Iterables.stream(null));
    }

    @Test
    void iterablesStream() {
        List<Object> list = asList(1, 2, "3", '4', null, "abc", "56789");

        assertEquals(list, Iterables.stream(list).collect(toList()));
    }

    @Test
    void iterablesStreamClosesResourceIterator() {
        List<Object> list = asList("a", "b", "c", "def");

        Resource resource = mock(Resource.class);
        ResourceIterable<Object> iterable = new ResourceIterable<>() {
            @Override
            public ResourceIterator<Object> iterator() {
                return Iterators.resourceIterator(list.iterator(), resource);
            }

            @Override
            public void close() {
                // no-op
            }
        };

        try (Stream<Object> stream = Iterables.stream(iterable)) {
            assertEquals(list, stream.collect(toList()));
        }
        verify(resource).close();
    }

    @Test
    void iterablesStreamCharacteristics() {
        Iterable<Integer> iterable = asList(1, 2, 3);
        int characteristics = Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.NONNULL;

        Stream<Integer> stream = Iterables.stream(iterable, characteristics);

        assertEquals(characteristics, stream.spliterator().characteristics());
    }

    @Test
    void iteratorsToString() {
        assertEquals("[a, b, c]", Iterators.toString(Iterators.iterator("a", "b", "c"), Object::toString, 5));
        assertEquals("[a, b, ...]", Iterators.toString(Iterators.iterator("a", "b", "c"), Object::toString, 2));
    }

    @Test
    void iteratorsToStringClosesResourceIterator() {
        // given
        var closed = new MutableBoolean();
        var iterator = Iterators.resourceIterator(Iterators.iterator("a", "b", "c"), closed::setTrue);

        // when
        var str = Iterators.toString(iterator, Object::toString, 5);

        // then
        assertThat(closed.getValue()).isTrue();
        assertThat(str).isEqualTo("[a, b, c]");
    }

    @Test
    void forEachRemaining() {
        // given
        var items = Sets.newHashSet();
        // when
        Iterators.forEachRemaining(Iterators.iterator("a", "b", "c"), items::add);
        // then
        assertThat(items).containsExactly("a", "b", "c");
    }

    @Test
    void forEachRemainingWithAlreadyConsumed() {
        // given
        var items = Sets.newHashSet();
        Iterator<String> iterator = Iterators.iterator("a", "b", "c");
        // when
        iterator.next();
        Iterators.forEachRemaining(iterator, items::add);
        // then
        assertThat(items).containsExactly("b", "c");
    }

    @Test
    void forEachRemainingAllAlreadyConsumed() {
        // given
        var items = Sets.newHashSet();
        Iterator<String> iterator = Iterators.iterator("a", "b", "c");
        // when
        iterator.next();
        iterator.next();
        iterator.next();
        Iterators.forEachRemaining(iterator, items::add);
        // then
        assertThat(items).isEmpty();
    }

    @Test
    void forEachRemainingClosesResourceIterator() {
        // given
        var closed = new MutableBoolean();
        var items = Sets.newHashSet();
        var iterator = Iterators.resourceIterator(Iterators.iterator("a", "b", "c"), closed::setTrue);

        // when
        Iterators.forEachRemaining(iterator, items::add);

        // then
        assertThat(closed.getValue()).isTrue();
        assertThat(items).containsExactly("a", "b", "c");
    }

    @Test
    void iteratorsEqual() {
        // given
        var items1 = List.of(1, 2, 3);
        var items2 = List.of(1, 2);
        var items3 = List.of(1, 2, 3, 4);

        // when / then
        assertThat(Iterators.iteratorsEqual(items1.iterator(), items1.iterator()))
                .isTrue();
        assertThat(Iterators.iteratorsEqual(items1.iterator(), items2.iterator()))
                .isFalse();
        assertThat(Iterators.iteratorsEqual(items1.iterator(), items3.iterator()))
                .isFalse();
        assertThat(Iterators.iteratorsEqual(items2.iterator(), items1.iterator()))
                .isFalse();
        assertThat(Iterators.iteratorsEqual(items3.iterator(), items1.iterator()))
                .isFalse();
    }

    @Test
    void iteratorsEqualClosesResourceIterator() {
        // given
        var closed1 = new MutableBoolean();
        var closed2 = new MutableBoolean();
        var iterator1 = Iterators.resourceIterator(Iterators.iterator("a", "b", "c"), closed1::setTrue);
        var iterator2 = Iterators.resourceIterator(Iterators.iterator("a", "d"), closed2::setTrue);

        // when
        assertThat(Iterators.iteratorsEqual(iterator1, iterator2)).isFalse();

        // then
        assertThat(closed1.getValue()).isTrue();
        assertThat(closed2.getValue()).isTrue();
    }

    @Test
    void iteratorSkipDiscardsItems() {
        final var iterator = Iterators.iterator("a", "b", "c", "d", "e");

        Iterators.skip(iterator, 3);
        assertThat(iterator.next()).isEqualTo("d");
        assertThat(iterator.next()).isEqualTo("e");
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void longIteratorSkipDiscardsItems() {
        final var items = LongArrayList.newListWith(1, 2, 3, 4, 5);
        final var iterator = items.longIterator();

        Iterators.skip(iterator, 3);
        assertThat(iterator.next()).isEqualTo(4);
        assertThat(iterator.next()).isEqualTo(5);
        assertThat(iterator.hasNext()).isFalse();
    }
}
