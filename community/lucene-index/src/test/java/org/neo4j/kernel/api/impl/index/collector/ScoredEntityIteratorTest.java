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
package org.neo4j.kernel.api.impl.index.collector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Test;

class ScoredEntityIteratorTest {
    @Test
    void mergeShouldReturnOrderedResults() {
        StubValuesIterator one = new StubValuesIterator().add(3, 10).add(10, 3).add(12, 1);
        StubValuesIterator two = new StubValuesIterator()
                .add(1, 12)
                .add(5, 8)
                .add(7, 6)
                .add(8, 5)
                .add(11, 2);
        StubValuesIterator three =
                new StubValuesIterator().add(2, 11).add(4, 9).add(6, 7).add(9, 4);

        ValuesIterator concat = ScoredEntityIterator.mergeIterators(Lists.fixedSize.of(one, two, three));

        for (int i = 1; i <= 12; i++) {
            assertThat(concat.hasNext()).isTrue();
            assertThat(concat.next()).isEqualTo(concat.current()).isEqualTo(i);
            assertThat(concat.currentScore()).isEqualTo(13 - i, offset(0.001f));
        }
        assertThat(concat.hasNext()).isFalse();
    }

    @Test
    void mergeShouldCorrectlyOrderSpecialValues() {
        // According to CIP2016-06-14, NaN comes between positive infinity and the largest float/double value.
        StubValuesIterator one = new StubValuesIterator()
                .add(2, Float.POSITIVE_INFINITY)
                .add(4, 1.0f)
                .add(6, Float.MIN_VALUE)
                .add(8, -1.0f);
        StubValuesIterator two = new StubValuesIterator()
                .add(1, Float.NaN)
                .add(3, Float.MAX_VALUE)
                .add(5, Float.MIN_NORMAL)
                .add(7, 0.0f)
                .add(9, Float.NEGATIVE_INFINITY);

        ValuesIterator concat = ScoredEntityIterator.mergeIterators(Lists.fixedSize.of(one, two));

        assertThat(concat.hasNext()).isTrue();
        assertThat(concat.next()).isEqualTo(1);
        assertThat(concat.currentScore()).isNaN();

        assertThat(concat.next()).isEqualTo(2);
        assertThat(concat.currentScore()).isInfinite().isPositive();

        assertThat(concat.next()).isEqualTo(3);
        assertThat(concat.next()).isEqualTo(4);
        assertThat(concat.next()).isEqualTo(5);
        assertThat(concat.next()).isEqualTo(6);
        assertThat(concat.next()).isEqualTo(7);
        assertThat(concat.next()).isEqualTo(8);
        assertThat(concat.next()).isEqualTo(9);
        assertThat(concat.hasNext()).isFalse();
    }

    @Test
    void mergeShouldHandleEmptyIterators() {
        ValuesIterator one = new StubValuesIterator();
        ValuesIterator two =
                new StubValuesIterator().add(1, 5).add(2, 4).add(3, 3).add(4, 2).add(5, 1);
        ValuesIterator three = new StubValuesIterator();

        ValuesIterator concat = ScoredEntityIterator.mergeIterators(Lists.fixedSize.of(one, two, three));

        for (int i = 1; i <= 5; i++) {
            assertThat(concat.hasNext()).isTrue();
            assertThat(concat.next()).isEqualTo(i);
            assertThat(concat.current()).isEqualTo(i);
            assertThat(concat.currentScore()).isEqualTo(6 - i, offset(0.001f));
        }
        assertThat(concat.hasNext()).isFalse();
    }

    @Test
    void mergeShouldHandleAllEmptyIterators() {
        ValuesIterator one = new StubValuesIterator();
        ValuesIterator two = new StubValuesIterator();
        ValuesIterator three = new StubValuesIterator();

        ValuesIterator concat = ScoredEntityIterator.mergeIterators(Lists.fixedSize.of(one, two, three));
        assertThat(concat.hasNext()).isFalse();
    }
}
