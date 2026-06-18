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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    void firstElement() {
        Object object = new Object();
        Object object2 = new Object();
        Object defaultValue = new Object();

        // first Iterable
        assertThat(Iterables.first(asList(object, object2))).isEqualTo(object);
        assertThat(Iterables.first(singletonList(object))).isEqualTo(object);
        assertThatThrownBy(() -> Iterables.first(Collections.emptyList())).isInstanceOf(NoSuchElementException.class);

        // first Iterator
        assertThat(Iterators.first(asList(object, object2).iterator())).isEqualTo(object);
        assertThat(Iterators.first(singletonList(object).iterator())).isEqualTo(object);
        assertThatThrownBy(() -> Iterators.first(Collections.emptyIterator()))
                .isInstanceOf(NoSuchElementException.class);

        // firstOrNull Iterable
        assertThat(Iterables.firstOrNull(asList(object, object2))).isEqualTo(object);
        assertThat(Iterables.firstOrNull(singletonList(object))).isEqualTo(object);
        assertThat((Object) Iterables.firstOrNull(Collections.emptyList())).isNull();

        // firstOrNull Iterator
        assertThat(Iterators.firstOrNull(asList(object, object2).iterator())).isEqualTo(object);
        assertThat(Iterators.firstOrNull(singletonList(object).iterator())).isEqualTo(object);
        assertThat((Object) Iterators.firstOrNull(Collections.emptyIterator())).isNull();

        // firstOrDefault
        assertThat(Iterators.firstOrDefault(asList(object, object2).iterator(), defaultValue))
                .isEqualTo(object);
        assertThat(Iterators.firstOrDefault(singletonList(object).iterator(), defaultValue))
                .isEqualTo(object);
        assertThat(Iterators.firstOrDefault(Collections.emptyIterator(), defaultValue))
                .isEqualTo(defaultValue);
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
    void lastElement() {
        Object object = new Object();
        Object object2 = new Object();

        // last Iterable
        assertThat(Iterables.last(asList(object, object2))).isEqualTo(object2);
        assertThat(Iterables.last(singletonList(object))).isEqualTo(object);
        assertThatThrownBy(() -> Iterables.last(Collections.emptyList())).isInstanceOf(NoSuchElementException.class);

        // last Iterator
        assertThat(Iterators.last(asList(object, object2).iterator())).isEqualTo(object2);
        assertThat(Iterators.last(singletonList(object).iterator())).isEqualTo(object);
        assertThatThrownBy(() -> Iterators.last(Collections.emptyIterator()))
                .isInstanceOf(NoSuchElementException.class);

        // lastOrNull Iterator
        assertThat(Iterators.lastOrNull(asList(object, object2).iterator())).isEqualTo(object2);
        assertThat(Iterators.lastOrNull(singletonList(object).iterator())).isEqualTo(object);
        assertThat((Object) Iterators.lastOrNull(Collections.emptyIterator())).isNull();
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
    void singleElement() {
        Object object = new Object();
        Object object2 = new Object();

        // single Iterable
        assertThat(Iterables.single(singletonList(object))).isEqualTo(object);
        assertThatThrownBy(() -> Iterables.single(Collections.emptyList())).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> Iterables.single(asList(object, object2))).isInstanceOf(NoSuchElementException.class);

        // single Iterator
        assertThat(Iterators.single(singletonList(object).iterator())).isEqualTo(object);
        assertThatThrownBy(() -> Iterators.single(Collections.emptyIterator()))
                .isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> Iterators.single(asList(object, object2).iterator()))
                .isInstanceOf(NoSuchElementException.class);

        // singleOrNull Iterable
        assertThat(Iterables.singleOrNull(singletonList(object))).isEqualTo(object);
        assertThat((Object) Iterables.singleOrNull(Collections.emptyList())).isNull();
        assertThatThrownBy(() -> Iterables.singleOrNull(asList(object, object2)))
                .isInstanceOf(NoSuchElementException.class);

        // singleOrNull Iterator
        assertThat(Iterators.singleOrNull(singletonList(object).iterator())).isEqualTo(object);
        assertThat((Object) Iterators.singleOrNull(Collections.emptyIterator())).isNull();
        assertThatThrownBy(() -> Iterators.singleOrNull(asList(object, object2).iterator()))
                .isInstanceOf(NoSuchElementException.class);
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
        assertThat(Iterators.fromEnd(ints.iterator(), 0)).isEqualTo((Integer) 9);
        assertThat(Iterators.fromEnd(ints.iterator(), 1)).isEqualTo((Integer) 8);
        assertThat(Iterators.fromEnd(ints.iterator(), 2)).isEqualTo((Integer) 7);
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
        assertThatThrownBy(() -> Iterators.stream(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void iteratorsStream() {
        List<Object> list = asList(1, 2, "3", '4', null, "abc", "56789");

        Iterator<Object> iterator = list.iterator();

        assertThat(Iterators.stream(iterator).toList()).containsExactlyElementsOf(list);
    }

    @Test
    void iteratorsStreamClosesResourceIterator() {
        List<Object> list = asList("a", "b", "c", "def");

        Resource resource = mock(Resource.class);
        ResourceIterator<Object> iterator = Iterators.resourceIterator(list.iterator(), resource);

        try (Stream<Object> stream = Iterators.stream(iterator)) {
            assertThat(stream.toList()).containsExactlyElementsOf(list);
        }
        verify(resource).close();
    }

    @Test
    void iteratorsStreamCharacteristics() {
        Iterator<Integer> iterator = asList(1, 2, 3).iterator();
        int characteristics = Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.SORTED;

        Stream<Integer> stream = Iterators.stream(iterator, characteristics);

        assertThat(stream.spliterator().characteristics()).isEqualTo(characteristics);
    }

    @Test
    void iterablesStreamForNull() {
        assertThatThrownBy(() -> Iterables.stream(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void iterablesStream() {
        List<Object> list = asList(1, 2, "3", '4', null, "abc", "56789");

        assertThat(Iterables.stream(list).toList()).containsExactlyElementsOf(list);
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
            assertThat(stream.toList()).containsExactlyElementsOf(list);
        }
        verify(resource).close();
    }

    @Test
    void iterablesStreamCharacteristics() {
        Iterable<Integer> iterable = asList(1, 2, 3);
        int characteristics = Spliterator.DISTINCT | Spliterator.ORDERED | Spliterator.NONNULL;

        Stream<Integer> stream = Iterables.stream(iterable, characteristics);

        assertThat(stream.spliterator().characteristics()).isEqualTo(characteristics);
    }

    @Test
    void iteratorsToString() {
        assertThat(Iterators.toString(Iterators.iterator("a", "b", "c"), Object::toString, 5))
                .isEqualTo("[a, b, c]");
        assertThat(Iterators.toString(Iterators.iterator("a", "b", "c"), Object::toString, 2))
                .isEqualTo("[a, b, ...]");
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
        assertThat(iterator).isExhausted();
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
