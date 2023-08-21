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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.helpers.collection.ResourceClosingIterator.newResourceIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;

public class AbstractResourceIterableTest {
    @Test
    void shouldDelegateToUnderlyingIterableForData() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);

        final var items = Arrays.asList(0, 1, 2);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return Iterators.resourceIterator(items.iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };
        final var iterator = iterable.iterator();

        // Then
        assertThat(Iterators.asList(iterator)).containsExactlyElementsOf(items);
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 10})
    void callToIteratorShouldCreateNewIterators(int numberOfIterators) {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorCount = new MutableInt();

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                iteratorCount.increment();
                return Iterators.resourceIterator(Iterators.asIterator(0), Resource.EMPTY);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        final var iterators = new ArrayList<ResourceIterator<Integer>>();
        for (int i = 0; i < numberOfIterators; i++) {
            iterators.add(iterable.iterator());
        }
        iterable.close();

        // Then
        assertThat(iterableClosed.isTrue()).isTrue();
        assertThat(iteratorCount.getValue()).isEqualTo(numberOfIterators);
        assertThat(iterators).containsOnlyOnceElementsOf(new HashSet<>(iterators));
    }

    @Test
    void shouldCloseAllIteratorsIfCloseCalledOnIterable() {
        // Given
        final var iteratorsClosed = Arrays.asList(false, false, false, false);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            private int created;

            @Override
            protected ResourceIterator<Integer> newIterator() {
                var pos = created;
                created++;
                return Iterators.resourceIterator(Iterators.asIterator(0), () -> iteratorsClosed.set(pos, true));
            }
        };
        iterable.iterator();
        iterable.iterator();
        iterable.iterator();
        iterable.close();

        // Then
        assertThat(iteratorsClosed.get(0)).isTrue();
        assertThat(iteratorsClosed.get(1)).isTrue();
        assertThat(iteratorsClosed.get(2)).isTrue();
        assertThat(iteratorsClosed.get(3)).isFalse();
    }

    @Test
    void shouldCloseAllIteratorsEvenIfOnlySomeCloseCalled() {
        // Given
        final var iteratorsClosed = new MutableInt();

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return Iterators.resourceIterator(Iterators.asIterator(0), iteratorsClosed::increment);
            }
        };
        final var iterator1 = iterable.iterator();
        iterable.iterator();
        final var iterator2 = iterable.iterator();
        iterable.iterator();
        final var iterator3 = iterable.iterator();
        iterable.iterator();
        iterable.iterator();

        // go out of order
        iterator3.close();
        iterator1.close();
        iterator2.close();
        iterable.close();

        // Then
        assertThat(iteratorsClosed.getValue()).isEqualTo(7);
    }

    @Test
    void failIteratorCreationAfterIterableClosed() {
        // Given
        final var iteratorCreated = new MutableBoolean(false);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                iteratorCreated.setTrue();
                return Iterators.emptyResourceIterator();
            }
        };
        iterable.close();

        // Then
        assertThatThrownBy(iterable::iterator);
        assertThat(iteratorCreated.isTrue()).isFalse();
    }

    @Test
    void shouldCloseIteratorIfCloseCalled() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorCreated = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                iteratorCreated.setTrue();
                return Iterators.resourceIterator(List.of(0).iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };
        assertThat(iterable.iterator().hasNext()).isTrue();
        iterable.close();

        // Then
        assertThat(iteratorCreated.isTrue()).isTrue();
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isTrue();
    }

    @Test
    void shouldCloseIteratorOnForEachFailure() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);

        @SuppressWarnings("unchecked")
        final var intIterator = (Iterator<Integer>) mock(Iterator.class);
        when(intIterator.hasNext()).thenReturn(true).thenReturn(true);
        when(intIterator.next()).thenReturn(1).thenThrow(IllegalStateException.class);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return Iterators.resourceIterator(intIterator, iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // Then
        final var emitted = new ArrayList<Integer>();
        assertThatThrownBy(() -> {
            try (iterable) {
                for (var item : iterable) {
                    emitted.add(item);
                }
            }
        });
        assertThat(emitted).isEqualTo(List.of(1));
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isTrue();
    }

    @Test
    void shouldCloseIteratorOnForEachCompletion() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);

        final var items = Arrays.asList(0, 1, 2);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return Iterators.resourceIterator(items.iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        final var emitted = new ArrayList<Integer>();
        for (var item : iterable) {
            emitted.add(item);
        }

        // Then
        assertThat(emitted).isEqualTo(items);
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isFalse();
    }

    @Test
    void streamShouldCloseIteratorAndIterable() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);
        final var resourceIterator = newResourceIterator(iterator(new Integer[] {1, 2, 3}), iteratorClosed::setTrue);

        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return resourceIterator;
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // When
        try (Stream<Integer> stream = iterable.stream()) {
            final var result = stream.toList();
            assertThat(result).isEqualTo(asList(1, 2, 3));
        }

        // Then
        assertThat(iterableClosed.isTrue()).isTrue();
        assertThat(iteratorClosed.isTrue()).isTrue();
    }

    @Test
    void streamShouldCloseMultipleOnCompleted() {
        // Given
        final var closed = new MutableInt();
        Resource resource = closed::incrementAndGet;
        final var resourceIterator = newResourceIterator(iterator(new Integer[] {1, 2, 3}), resource, resource);

        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return resourceIterator;
            }
        };

        // When
        final var result = iterable.stream().toList();

        // Then
        assertThat(result).isEqualTo(asList(1, 2, 3));
        assertThat(closed.intValue()).isEqualTo(2);
    }
}
