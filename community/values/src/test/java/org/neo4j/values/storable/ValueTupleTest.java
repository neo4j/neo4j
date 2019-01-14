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
package org.neo4j.values.storable;

import org.junit.Test;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

public class ValueTupleTest
{
    @Test
    public void shouldEqual()
    {
        assertEquals( tuple( true ), tuple( true ) );
        assertNotEquals( tuple( true ), tuple( false ) );
        assertEquals( tuple( 1, 2, 3, 4L ), tuple( 1.0, 2.0, 3, (byte)4 ) );
        assertNotEquals( tuple( 2, 3, 1 ), tuple( 1, 2, 3 ) );
        assertNotEquals( tuple( 1, 2, 3, 4 ), tuple( 1, 2, 3 ) );
        assertNotEquals( tuple( 1, 2, 3 ), tuple( 1, 2, 3, 4 ) );
        assertEquals( tuple( (Object) new int[]{3} ), tuple( (Object) new int[]{3} ) );
        assertEquals( tuple( (Object) new int[]{3} ), tuple( (Object) new byte[]{3} ) );
        assertEquals( tuple( 'a', new int[]{3}, "c" ), tuple( 'a', new int[]{3}, "c" ) );
    }

    private ValueTuple tuple( Object... objs )
    {
        Value[] values = new Value[objs.length];
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = Values.of( objs[i] );
        }
        return ValueTuple.of( values );
    }

    private void assertEquals( ValueTuple a, ValueTuple b )
    {
        assertThat( a, equalTo( b ) );
        assertThat( b, equalTo( a ) );
        assertTrue( format( "Expected hashCode for %s and %s to be equal", a, b ), a.hashCode() == b.hashCode() );
    }

    private void assertNotEquals( ValueTuple a, ValueTuple b )
    {
        assertThat( a, not( equalTo( b ) ) );
        assertThat( b, not( equalTo( a ) ) );
    }
}
