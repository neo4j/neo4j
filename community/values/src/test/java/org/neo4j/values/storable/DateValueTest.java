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
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.DateValue.ordinalDate;
import static org.neo4j.values.storable.DateValue.parse;
import static org.neo4j.values.storable.DateValue.quarterDate;
import static org.neo4j.values.storable.DateValue.weekDate;

@SuppressWarnings( "ThrowableNotThrown" )
public class DateValueTest
{
    @Test
    public void shouldParseYear() throws Exception
    {
        assertEquals( date( 2015, 1, 1 ), parse( "2015" ) );
        assertEquals( date( 2015, 1, 1 ), parse( "+2015" ) );
        assertEquals( date( 2015, 1, 1 ), parse( "+0002015" ) );
    }

    @Test
    public void shouldParseYearMonth() throws Exception
    {
        assertEquals( date( 2015, 3, 1 ), parse( "201503" ) );
        assertEquals( date( 2015, 3, 1 ), parse( "2015-03" ) );
        assertEquals( date( 2015, 3, 1 ), parse( "2015-3" ) );
        assertEquals( date( 2015, 3, 1 ), parse( "+2015-03" ) );
    }

    @Test
    public void shouldParseYearWeek() throws Exception
    {
        assertEquals( weekDate( 2015, 5, 1 ), parse( "2015W05" ) );
        assertEquals( weekDate( 2015, 53, 1 ), parse( "2015W53" ) ); // 2015 had 53 weeks
        assertCannotParse( "2015W5" );
        assertEquals( weekDate( 2015, 5, 1 ), parse( "2015-W05" ) );
        assertEquals( weekDate( 2015, 5, 1 ), parse( "2015-W5" ) );
        assertEquals( weekDate( 2015, 5, 1 ), parse( "+2015-W05" ) );
        assertCannotParse( "+2015W05" );
    }

    @Test
    public void shouldParseYearQuarter() throws Exception
    {
        assumeTrue( DateValue.QUARTER_DATES );
        assertEquals( quarterDate( 2017, 3, 1 ), parse( "2017Q3" ) );
        assertEquals( quarterDate( 2017, 3, 1 ), parse( "2017-Q3" ) );
        assertEquals( quarterDate( 2017, 3, 1 ), parse( "+2017-Q3" ) );
    }

    @Test
    public void shouldParseCalendarDate() throws Exception
    {
        assertEquals( date( 2016, 1, 27 ), parse( "20160127" ) );
        assertEquals( date( 2016, 1, 27 ), parse( "+2016-01-27" ) );
        assertEquals( date( 2016, 1, 27 ), parse( "+2016-1-27" ) );
    }

    @Test
    public void shouldParseWeekDate() throws Exception
    {
        assertEquals( weekDate( 2015, 5, 6 ), parse( "2015W056" ) );
        assertCannotParse( "+2015W056" );
        assertEquals( weekDate( 2015, 5, 6 ), parse( "2015-W05-6" ) );
        assertEquals( weekDate( 2015, 5, 6 ), parse( "+2015-W05-6" ) );
        assertEquals( weekDate( 2015, 5, 6 ), parse( "2015-W5-6" ) );
        assertEquals( weekDate( 2015, 5, 6 ), parse( "+2015-W5-6" ) );
    }

    @Test
    public void shouldParseQuarterDate() throws Exception
    {
        assumeTrue( DateValue.QUARTER_DATES );
        assertEquals( quarterDate( 2017, 3, 92 ), parse( "2017Q392" ) );
        assertEquals( quarterDate( 2017, 3, 92 ), parse( "2017-Q3-92" ) );
        assertEquals( quarterDate( 2017, 3, 92 ), parse( "+2017-Q3-92" ) );
    }

    @Test
    public void shouldParseOrdinalDate() throws Exception
    {
        assertEquals( ordinalDate( 2017, 3 ), parse( "2017003" ) );
        assertCannotParse( "20173" );
        assertEquals( ordinalDate( 2017, 3 ), parse( "2017-003" ) );
        assertEquals( ordinalDate( 2017, 3 ), parse( "+2017-003" ) );
    }

    @Test
    public void shouldEnforceStrictWeekRanges() throws Exception
    {
        LocalDate localDate = weekDate( 2017, 52, 7 ).temporal();
        assertEquals( "Sunday is the seventh day of the week.", DayOfWeek.SUNDAY, localDate.getDayOfWeek() );
        assertEquals( 52, localDate.get( IsoFields.WEEK_OF_WEEK_BASED_YEAR ) );
        assertEquals( localDate, date( 2017, 12, 31 ).temporal() );
        try
        {
            DateValue value = weekDate( 2017, 53, 1 );
            fail( String.format(
                    "2017 does not have 53 weeks, %s is week %s of %s", value,
                    value.temporal().get( IsoFields.WEEK_OF_WEEK_BASED_YEAR ),
                    value.temporal().get( IsoFields.WEEK_BASED_YEAR ) ) );
        }
        catch ( DateTimeException expected )
        {
            assertEquals( "Year 2017 does not contain 53 weeks.", expected.getMessage() );
        }
        assertEquals( date( 2016, 1, 1 ), weekDate( 2015, 53, 5 ) );
    }

    @Test
    public void shouldEnforceStrictQuarterRanges() throws Exception
    {
        assertEquals( date( 2017, 3, 31 ), quarterDate( 2017, 1, 90 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 1, 0 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 2, 0 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 3, 0 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 4, 0 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 4, 93 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 3, 93 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 2, 92 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 1, 92 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2017, 1, 91 ) );
        assertEquals( date( 2016, 3, 31 ), quarterDate( 2016, 1, 91 ) );
        assertThrows( DateTimeException.class, () -> quarterDate( 2016, 1, 92 ) );
    }

    @Test
    public void shouldNotParseInvalidDates() throws Exception
    {
        assertCannotParse( "2015W54" ); // no year should have more than 53 weeks (2015 had 53 weeks)
        assertCannotParse( "2017W53" ); // 2017 only has 52 weeks
    }

    @Test
    public void shouldWriteDate() throws Exception
    {
        // given
        for ( DateValue value : new DateValue[] {
                date( 2016, 2, 29 ),
                date( 2017, 12, 22 ),
        } )
        {
            List<DateValue> values = new ArrayList<>( 1 );
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly()
            {
                @Override
                public void writeDate( long epochDay ) throws RuntimeException
                {
                    values.add( epochDate( epochDay ) );
                }
            };

            // when
            value.writeTo( writer );

            // then
            assertEquals( singletonList( value ), values );
        }
    }

    @SuppressWarnings( "UnusedReturnValue" )
    private DateTimeException assertCannotParse( String text )
    {
        DateValue value;
        try
        {
            value = parse( text );
        }
        catch ( DateTimeException e )
        {
            return e;
        }
        throw new AssertionError( String.format( "'%s' parsed to %s", text, value ) );
    }

    private static <X extends Exception, T> X assertThrows( Class<X> exception, Supplier<T> thunk )
    {
        T value;
        try
        {
            value = thunk.get();
        }
        catch ( Exception e )
        {
            if ( exception.isInstance( e ) )
            {
                return exception.cast( e );
            }
            else
            {
                throw new AssertionError( "Expected " + exception.getName(), e );
            }
        }
        throw new AssertionError( "Expected " + exception.getName() + " but returned: " + value );
    }
}
