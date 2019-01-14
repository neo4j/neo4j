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
package org.neo4j.kernel.impl.util.collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ContinuableArrayCursorTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldNotReturnAnyElementOnEmptySupplier()
    {
        // given
        ContinuableArrayCursor cursor = new ContinuableArrayCursor<>( () -> null );

        // then
        assertFalse( cursor.next() );
    }

    @Test
    public void shouldNotReturnAnyElementOnSupplierWithOneEmptyArray()
    {
        // given
        ContinuableArrayCursor cursor = new ContinuableArrayCursor( supply( new Integer[0] ) );

        // then
        assertFalse( cursor.next() );
    }

    @Test
    public void shouldMoveCursorOverSingleArray()
    {
        // given
        Integer[] array = new Integer[]{1, 2, 3};
        ContinuableArrayCursor<Integer> cursor = new ContinuableArrayCursor<>( supply( array ) );

        // then
        assertCursor( cursor, array );
    }

    @Test
    public void shouldMoveCursorOverMultipleArrays()
    {
        // given
        Integer[][] arrays = new Integer[][]{
                new Integer[]{1, 2, 3},
                new Integer[]{4, 5, 6},
                new Integer[]{7}
        };
        ContinuableArrayCursor<Integer> cursor = new ContinuableArrayCursor<>( supply( arrays ) );

        // then
        assertCursor( cursor, arrays );
    }

    @Test
    public void callGetBeforeNextShouldThrowIllegalStateException()
    {
        // given
        ContinuableArrayCursor<?> cursor = new ContinuableArrayCursor( supply( new Integer[0] ) );

        // then
        thrown.expect( IllegalStateException.class );
        cursor.get();
    }

    @Test
    public void callGetAfterNextReturnsFalseShouldThrowIllegalStateException()
    {
        // given
        ContinuableArrayCursor<Integer> cursor = new ContinuableArrayCursor<>( supply( new Integer[0] ) );

        // when
        assertFalse( cursor.next() );

        // then
        thrown.expect( IllegalStateException.class );
        cursor.get();
    }

    private Supplier<Integer[]> supply( Integer[] array )
    {
        return supply( new Integer[][]{ array } );
    }

    private Supplier<Integer[]> supply( Integer[][] arrays )
    {
        Iterator<Integer[]> iterator = Arrays.asList( arrays ).iterator();
        return () -> iterator.hasNext() ?
                                             iterator.next() : null;
    }

    private void assertCursor( ContinuableArrayCursor<?> cursor, Object[]... arrays )
    {
        for ( Object[] array : arrays )
        {
            for ( Object obj : array )
            {
                assertTrue( cursor.next() );
                assertEquals( obj, cursor.get() );
            }
        }
        assertFalse( cursor.next() );
    }
}
