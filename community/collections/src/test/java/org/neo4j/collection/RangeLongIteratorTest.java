/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.collection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RangeLongIteratorTest
{
    @Test
    void shouldIterateOverSubsetOfData()
    {
        // given
        long[] array = new long[]{1L, 2L, 3L, 4L, 5L};

        // when
        RangeLongIterator iterator = new RangeLongIterator( array, 2, 2 );

        // then
        assertThat( iterator.next() ).isEqualTo( 3L );
        assertThat( iterator.next() ).isEqualTo( 4L );
        assertThat( iterator.hasNext() ).isEqualTo( false );
    }

    @Test
    void shouldNotBeAbleToCreateInvalidRanges()
    {
        // given
        long[] array = new long[]{1L, 2L, 3L, 4L, 5L};

        // expect
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( array, -1, 0 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( array, 0, -1 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( array, 10, 2 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( array, 0, 12 ) );
        assertThrows( IllegalArgumentException.class, () -> new RangeLongIterator( array, 4, 4 ) );
    }

}
