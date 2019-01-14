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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.utils.ValueMath.overflowSafeAdd;
import static org.neo4j.values.utils.ValueMath.overflowSafeMultiply;
import static org.neo4j.values.utils.ValueMath.overflowSafeSubtract;

public class NumberValueMathTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

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
    public void shouldMultiplySimpleIntegers()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : integers )
            {
                assertThat( a.times( b ), equalTo( longValue( 42 * 42 ) ) );
                assertThat( b.times( a ), equalTo( longValue( 42 * 42 ) ) );
            }
        }
    }

    @Test
    public void shouldAddSimpleFloats()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};
        NumberValue[] floats =
                new NumberValue[]{floatValue( 42 ), doubleValue( 42 )};

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
    public void shouldSubtractSimpleFloats()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};
        NumberValue[] floats =
                new NumberValue[]{floatValue( 42 ), doubleValue( 42 )};

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : floats )
            {
                assertThat( a.minus( b ), equalTo( doubleValue( 0 ) ) );
                assertThat( b.minus( a ), equalTo( doubleValue( 0 ) ) );
            }
        }
    }

    @Test
    public void shouldMultiplySimpleFloats()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};
        NumberValue[] floats =
                new NumberValue[]{floatValue( 42 ), doubleValue( 42 )};

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : floats )
            {
                assertThat( a.times( b ), equalTo( doubleValue( 42 * 42 ) ) );
                assertThat( b.times( a ), equalTo( doubleValue( 42 * 42 ) ) );
            }
        }
    }

    @Test
    public void shouldDivideSimpleIntegers()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : integers )
            {
                assertThat( a.divideBy( b ), equalTo( longValue( 1 ) ) );
                assertThat( b.divideBy( a ), equalTo( longValue( 1 ) ) );
            }
        }
    }

    @Test
    public void shouldDivideSimpleFloats()
    {
        NumberValue[] integers =
                new NumberValue[]{byteValue( (byte) 42 ), shortValue( (short) 42 ), intValue( 42 ), longValue( 42 )};
        NumberValue[] floats =
                new NumberValue[]{floatValue( 42 ), doubleValue( 42 )};

        for ( NumberValue a : integers )
        {
            for ( NumberValue b : floats )
            {
                assertThat( a.divideBy( b ), equalTo( doubleValue( 1.0 ) ) );
                assertThat( b.divideBy( a ), equalTo( doubleValue( 1.0 ) ) );
            }
        }
    }

    @Test
    public void shouldFailOnOverflowingAdd()
    {
        //Expect
        exception.expect( ArithmeticException.class );

        //WHEN
        longValue( Long.MAX_VALUE ).plus( longValue( 1 ) );
    }

    @Test
    public void shouldFailOnOverflowingSubtraction()
    {
        //Expect
        exception.expect( ArithmeticException.class );

        //WHEN
        longValue( Long.MAX_VALUE ).minus( longValue( -1 ) );
    }

    @Test
    public void shouldFailOnOverflowingMultiplication()
    {
        //Expect
        exception.expect( ArithmeticException.class );

        //When
        longValue( Long.MAX_VALUE ).times( 2 );
    }

    @Test
    public void shouldNotOverflowOnSafeAddition()
    {
        assertThat( overflowSafeAdd( Long.MAX_VALUE, 1 ), equalTo( doubleValue( (double) Long.MAX_VALUE + 1 ) ) );
    }

    @Test
    public void shouldNotOverflowOnSafeSubtraction()
    {
        assertThat( overflowSafeSubtract( Long.MAX_VALUE, -1 ), equalTo( doubleValue( ((double) Long.MAX_VALUE)  + (double) 1 ) ) );
    }

    @Test
    public void shouldNotOverflowOnMultiplication()
    {
        assertThat( overflowSafeMultiply( Long.MAX_VALUE, 2 ), equalTo( doubleValue( (double) Long.MAX_VALUE * 2 ) ) );
    }
}
