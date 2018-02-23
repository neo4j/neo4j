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

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.LocalTimeValue.parse;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class LocalTimeValueTest
{
    @Test
    public void shouldParseTimeWithOnlyHour()
    {
        assertEquals( localTime( 14, 0, 0, 0 ), parse( "14" ) );
        assertEquals( localTime( 4, 0, 0, 0 ), parse( "4" ) );
        assertEquals( localTime( 4, 0, 0, 0 ), parse( "04" ) );
    }

    @Test
    public void shouldParseTimeWithHourAndMinute()
    {
        assertEquals( localTime( 14, 5, 0, 0 ), parse( "1405" ) );
        assertEquals( localTime( 14, 5, 0, 0 ), parse( "14:5" ) );
        assertEquals( localTime( 4, 15, 0, 0 ), parse( "4:15" ) );
        assertEquals( localTime( 9, 7, 0, 0 ), parse( "9:7" ) );
        assertEquals( localTime( 3, 4, 0, 0 ), parse( "03:04" ) );
    }

    @Test
    public void shouldParseTimeWithHourMinuteAndSecond()
    {
        assertEquals( localTime( 14, 5, 17, 0 ), parse( "140517" ) );
        assertEquals( localTime( 14, 5, 17, 0 ), parse( "14:5:17" ) );
        assertEquals( localTime( 4, 15, 4, 0 ), parse( "4:15:4" ) );
        assertEquals( localTime( 9, 7, 19, 0 ), parse( "9:7:19" ) );
        assertEquals( localTime( 3, 4, 1, 0 ), parse( "03:04:01" ) );
    }

    @Test
    public void shouldParseTimeWithHourMinuteSecondAndFractions()
    {
        assertEquals( localTime( 14, 5, 17, 123000000 ), parse( "140517.123" ) );
        assertEquals( localTime( 14, 5, 17, 1 ), parse( "14:5:17.000000001" ) );
        assertEquals( localTime( 4, 15, 4, 0 ), parse( "4:15:4.000" ) );
        assertEquals( localTime( 9, 7, 19, 999999999 ), parse( "9:7:19.999999999" ) );
        assertEquals( localTime( 3, 4, 1, 123456789 ), parse( "03:04:01.123456789" ) );
    }

    @Test
    @SuppressWarnings( "ThrowableNotThrown" )
    public void shouldFailToParseTimeOutOfRange()
    {
        assertCannotParse( "24" );
        assertCannotParse( "1760" );
        assertCannotParse( "173260" );
        assertCannotParse( "173250.0000000001" );
    }

    @Test
    public void shouldWriteLocalTime()
    {
        // given
        for ( LocalTimeValue value : new LocalTimeValue[] {
                localTime( 0, 0, 0, 0 ),
                localTime( 11, 22, 33, 123456789 ),
                localTime( 2, 3, 4, 5 ),
                localTime( 23, 59, 59, 999999999 ),
        } )
        {
            List<LocalTimeValue> values = new ArrayList<>( 1 );
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly()
            {
                @Override
                public void writeLocalTime( long nanoOfDay ) throws RuntimeException
                {
                    values.add( localTime( nanoOfDay ) );
                }
            };

            // when
            value.writeTo( writer );

            // then
            assertEquals( singletonList( value ), values );
        }
    }

    @Test
    public void shouldEqualItself()
    {
        assertEqual( localTime( 10, 52, 5, 6 ), localTime( 10, 52, 5, 6 ) );
    }

    @Test
    public void shouldNotEqualOther()
    {
        assertNotEqual( localTime( 10, 52, 5, 6 ), localTime( 10, 52, 5, 7 ) );
    }

    @SuppressWarnings( "UnusedReturnValue" )
    private DateTimeException assertCannotParse( String text )
    {
        try
        {
            parse( text );
        }
        catch ( DateTimeException e )
        {
            return e;
        }
        throw new AssertionError( text );
    }
}
