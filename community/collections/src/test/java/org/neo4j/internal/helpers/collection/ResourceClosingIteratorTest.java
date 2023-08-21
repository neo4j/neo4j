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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.ResourceIterator;

public class ResourceClosingIteratorTest {
    @Test
    void fromResourceIterableShouldCloseParentIterable() {
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
        ResourceIterator<Integer> iterator = ResourceClosingIterator.fromResourceIterable(iterable);

        // Then
        assertThat(Iterators.asList(iterator)).containsExactlyElementsOf(items);
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isTrue();
    }
}
