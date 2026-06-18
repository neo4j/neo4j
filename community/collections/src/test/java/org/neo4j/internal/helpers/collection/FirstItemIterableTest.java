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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class FirstItemIterableTest {
    @Test
    void emptyIterator() {
        FirstItemIterable<?> firstItemIterable = new FirstItemIterable<>(Collections.emptyList());
        Iterator<?> empty = firstItemIterable.iterator();
        assertThat(empty).isExhausted();
        assertThatThrownBy(empty::next).isInstanceOf(NoSuchElementException.class);
        assertThat(firstItemIterable.getFirst()).isNull();
    }

    @Test
    void singleIterator() {
        FirstItemIterable<Boolean> firstItemIterable = new FirstItemIterable<>(Collections.singleton(Boolean.TRUE));
        Iterator<Boolean> empty = firstItemIterable.iterator();
        assertThat(empty).hasNext();
        assertThat(empty.next()).isEqualTo(Boolean.TRUE);
        assertThat(firstItemIterable.getFirst()).isEqualTo(Boolean.TRUE);
        assertThat(empty).isExhausted();
        assertThatThrownBy(empty::next).isInstanceOf(NoSuchElementException.class);
        assertThat(firstItemIterable.getFirst()).isEqualTo(Boolean.TRUE);
    }

    @Test
    void multiIterator() {
        FirstItemIterable<Boolean> firstItemIterable = new FirstItemIterable<>(asList(Boolean.TRUE, Boolean.FALSE));
        Iterator<Boolean> empty = firstItemIterable.iterator();
        assertThat(empty).hasNext();
        assertThat(empty.next()).isEqualTo(Boolean.TRUE);
        assertThat(firstItemIterable.getFirst()).isEqualTo(Boolean.TRUE);
        assertThat(empty).hasNext();
        assertThat(empty.next()).isEqualTo(Boolean.FALSE);
        assertThat(firstItemIterable.getFirst()).isEqualTo(Boolean.TRUE);
        assertThat(empty).isExhausted();
        assertThatThrownBy(empty::next).isInstanceOf(NoSuchElementException.class);
        assertThat(firstItemIterable.getFirst()).isEqualTo(Boolean.TRUE);
    }
}
