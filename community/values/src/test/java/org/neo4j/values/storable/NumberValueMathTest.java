/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;

public class NumberValueMathTest
{
    @Test
    public void shouldAddSimpleIntegers()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : integers )
            {
                assertThat( a.plus( b ), equalTo( longValue( 84 ) ) );
                assertThat( b.plus( a ), equalTo( longValue( 84 ) ) );
            }
        }
    }

    @Test
    public void shouldSubtractSimpleIntegers()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : integers )
            {
                assertThat( a.minus( b ), equalTo( longValue( 0 ) ) );
                assertThat( b.minus( a ), equalTo( longValue( 0 ) ) );
            }
        }
    }

    @Test
    public void shouldAddSimpleFloats()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};
        NumberValue[] floats =
                new NumberValue[]{floatValue( 42 ), doubleValue( 42 ) };

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : floats )
            {
                assertThat( a.plus( b ), equalTo( doubleValue( 84 ) ) );
                assertThat( b.plus( a ), equalTo( doubleValue( 84 ) ) );
            }
        }
    }

    @Test
    public void shouldNotOverflowOnAddition()
    {
        assertThat( longValue( Long.MAX_VALUE ).plus( longValue( 1 ) ), equalTo( doubleValue( (double) Long.MAX_VALUE + 1 ) ) );
    }

    @Test
    public void shouldNotOverflowOnSubtraction()
    {
        assertThat( longValue( Long.MAX_VALUE ).minus( longValue( -1 ) ), equalTo( doubleValue( (double) Long.MAX_VALUE + 1 ) ) );
    }
}
