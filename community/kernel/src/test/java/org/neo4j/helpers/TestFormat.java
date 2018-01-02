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
package org.neo4j.helpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.Test;

public class TestFormat
{
    @Test
    public void dateTime() throws Exception
    {
        // Date
        long timeWithDate = System.currentTimeMillis();
        String dateAsString = Format.date( timeWithDate );
        assertEquals( timeWithDate, new SimpleDateFormat( Format.DATE_FORMAT ).parse( dateAsString ).getTime() );
        
        // Time
        String timeAsString = Format.time( timeWithDate );
        assertEquals( timeWithDate, translateToDate( timeWithDate,
                new SimpleDateFormat( Format.TIME_FORMAT ).parse( timeAsString ).getTime(), Format.DEFAULT_TIME_ZONE ) );
    }
    
    @Test
    public void dateTimeWithTimeZone() throws Exception
    {
        String zoneOffset = "+03:00";
        TimeZone zone = TimeZone.getTimeZone( "GMT" + zoneOffset );
        
        // Date
        String asString = Format.date( zone );
        assertTrue( asString.endsWith( withoutColon( zoneOffset ) ) );
        long timeWithDate = new SimpleDateFormat( Format.DATE_FORMAT ).parse( asString ).getTime();
        
        asString = Format.date( timeWithDate, zone );
        assertTrue( asString.endsWith( withoutColon( zoneOffset ) ) );
        assertEquals( timeWithDate, new SimpleDateFormat( Format.DATE_FORMAT ).parse( asString ).getTime() );

        asString = Format.date( new Date( timeWithDate ), zone );
        assertTrue( asString.endsWith( withoutColon( zoneOffset ) ) );
        assertEquals( timeWithDate, new SimpleDateFormat( Format.DATE_FORMAT ).parse( asString ).getTime() );
        
        // Time
        asString = Format.time( timeWithDate, zone );
        assertEquals( timeWithDate, translateToDate( timeWithDate,
                new SimpleDateFormat( Format.TIME_FORMAT ).parse( asString ).getTime(), zone ) );

        asString = Format.time( new Date( timeWithDate ), zone );
        assertEquals( timeWithDate, translateToDate( timeWithDate,
                new SimpleDateFormat( Format.TIME_FORMAT ).parse( asString ).getTime(), zone ) );
    }

    private long translateToDate( long timeWithDate, long time, TimeZone timeIsGivenInThisTimeZone )
    {
        Calendar calendar = Calendar.getInstance(timeIsGivenInThisTimeZone);
        calendar.setTimeInMillis( timeWithDate );
        
        Calendar timeCalendar = Calendar.getInstance();
        timeCalendar.setTimeInMillis( time );
        timeCalendar.setTimeZone( timeIsGivenInThisTimeZone );
        timeCalendar.set( Calendar.YEAR, calendar.get( Calendar.YEAR ) ); 
        timeCalendar.set( Calendar.MONTH, calendar.get( Calendar.MONTH ) ); 
        boolean crossedDayBoundary = !timeIsGivenInThisTimeZone.equals( Format.DEFAULT_TIME_ZONE ) &&
                timeCalendar.get( Calendar.HOUR_OF_DAY ) < calendar.get( Calendar.HOUR_OF_DAY );
        timeCalendar.set( Calendar.DAY_OF_MONTH, calendar.get( Calendar.DAY_OF_MONTH ) + (crossedDayBoundary ? 1 : 0) );
        return timeCalendar.getTimeInMillis();
    }
    
    private String withoutColon( String zoneOffset )
    {
        return zoneOffset.replaceAll( ":", "" );
    }
}
