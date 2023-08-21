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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.helpers.collection.ResourceClosingIterator.newResourceIterator;

import java.util.ArrayList;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

class IterablesTest {
    @Test
    void safeForAllShouldConsumeAllSubjectsRegardlessOfSuccess() {
        // given
        final var seenSubjects = new ArrayList<>();
        final var failedSubjects = new ArrayList<>();
        ThrowingConsumer<String, RuntimeException> consumer = s -> {
            seenSubjects.add(s);

            // Fail every other
            if (seenSubjects.size() % 2 == 1) {
                failedSubjects.add(s);
                throw new RuntimeException(s);
            }
        };
        final var subjects = asList("1", "2", "3", "4", "5");

        // when
        assertThatThrownBy(() -> Iterables.safeForAll(subjects, consumer))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("1")
                .hasSuppressedException(new RuntimeException("3"))
                .hasSuppressedException(new RuntimeException("5"));

        // then good
        assertThat(seenSubjects).isEqualTo(subjects);
        assertThat(failedSubjects).containsExactly("1", "3", "5");
    }

    @Test
    void resourceIterableShouldNotCloseIfNoIteratorCreated() {
        // Given
        final var closed = new MutableBoolean(false);
        final var resourceIterator = newResourceIterator(iterator(new Integer[0]), closed::setTrue);

        // When
        Iterables.resourceIterable(() -> resourceIterator).close();

        // Then
        assertThat(closed.isTrue()).isFalse();
    }

    @Test
    void resourceIterableShouldAlsoCloseIteratorIfResource() {
        // Given
        final var closed = new MutableBoolean(false);
        final var resourceIterator = newResourceIterator(iterator(new Integer[] {1}), closed::setTrue);

        // When
        try (ResourceIterable<Integer> integers = Iterables.resourceIterable(() -> resourceIterator)) {
            integers.iterator().next();
        }

        // Then
        assertThat(closed.isTrue()).isTrue();
    }

    @Test
    void forEachShouldProcessAllItemsAndClose() {
        // Given
        final var subjects = asList(1, 2, 3, 4, 5);
        final var closed = new MutableBoolean(false);
        final var resourceIterator = newResourceIterator(subjects.iterator(), closed::setTrue);
        final var seenSubjects = new ArrayList<>();

        // when
        Iterables.forEach(Iterables.resourceIterable(() -> resourceIterator), seenSubjects::add);

        // then good
        assertThat(seenSubjects).isEqualTo(subjects);
        assertThat(closed.isTrue()).isTrue();
    }

    @Test
    void forEachShouldBailOnFirstExceptionAndClose() {
        // Given
        final var subjects = asList(1, 2, 3, 4, 5);
        final var closed = new MutableBoolean(false);
        final var resourceIterator = newResourceIterator(subjects.iterator(), () -> closed.setTrue());
        final var seenSubjects = new ArrayList<>();
        final var failedSubjects = new ArrayList<>();

        var consumer = (Consumer<Integer>) s -> {
            seenSubjects.add(s);

            if (seenSubjects.size() % 3 == 2) {
                failedSubjects.add(s);
                throw new RuntimeException("Boom on " + s);
            }
        };

        // when
        assertThatThrownBy(() -> Iterables.forEach(Iterables.resourceIterable(() -> resourceIterator), consumer));

        // then good
        assertThat(seenSubjects).isEqualTo(asList(1, 2));
        assertThat(failedSubjects).containsExactly(2);
        assertThat(closed.isTrue()).isTrue();
    }

    @Test
    void count() {
        // Given
        final var subjects = asList(1, 2, 3, 4, 5);
        final var iteratorClosed = new MutableBoolean(false);
        final var iterableClosed = new MutableBoolean(false);
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return newResourceIterator(subjects.iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // when
        long count = Iterables.count(iterable);

        // then
        assertThat(count).isEqualTo(subjects.size());
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isTrue();
    }

    @Test
    void firstNoItems() {
        // Given
        final var iteratorClosed = new MutableBoolean(false);
        final var iterableClosed = new MutableBoolean(false);
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return newResourceIterator(Iterators.emptyResourceIterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // when
        assertThatThrownBy(() -> Iterables.first(iterable));

        // then
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isTrue();
    }

    @Test
    void firstWithItems() {
        // Given
        final var subjects = asList(1, 2, 3, 4, 5);
        final var iteratorClosed = new MutableBoolean(false);
        final var iterableClosed = new MutableBoolean(false);
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return newResourceIterator(subjects.iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // when
        long first = Iterables.first(iterable);

        // then
        assertThat(first).isEqualTo(1);
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isTrue();
    }
}
