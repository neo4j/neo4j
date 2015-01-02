/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Format
{
    public static String date()
    {
        return date( DEFAULT_TIME_ZONE );
    }

    public static String date( TimeZone timeZone )
    {
        return date( new Date(), timeZone );
    }
    
    public static String date( long millis )
    {
        return date( millis, DEFAULT_TIME_ZONE );
    }

    public static String date( long millis, TimeZone timeZone )
    {
        return date( new Date( millis ), timeZone );
    }
    
    public static String date( Date date )
    {
        return date( date, DEFAULT_TIME_ZONE );
    }

    public static String date( Date date, TimeZone timeZone )
    {
        return DATE.format( date, timeZone );
    }
    
    public static String time()
    {
        return time( DEFAULT_TIME_ZONE );
    }

    public static String time( TimeZone timeZone )
    {
        return time( new Date() );
    }
    
    public static String time( long millis )
    {
        return time( millis, DEFAULT_TIME_ZONE );
    }

    public static String time( long millis, TimeZone timeZone )
    {
        return time( new Date( millis ), timeZone );
    }
    
    public static String time( Date date )
    {
        return time( date, DEFAULT_TIME_ZONE );
    }

    public static String time( Date date, TimeZone timeZone )
    {
        return TIME.format( date, timeZone );
    }
    
    public static String bytes( long bytes )
    {
        double size = bytes;
        for ( String suffix : BYTE_SIZES )
        {
            if ( size < 1024 ) return String.format( "%.2f %s", Double.valueOf( size ), suffix );
            size /= 1024;
        }
        return String.format( "%.2f TB", Double.valueOf( size ) );
    }

    private Format()
    {
        // No instances
    }

    private static final String[] BYTE_SIZES = { "B", "kB", "MB", "GB" };

    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSZ";
    public static final String TIME_FORMAT = "HH:mm:ss.SSS";
    
    /**
     * Default time zone is UTC (+00:00) so that comparing timestamped logs from different
     * sources is an easier task.
     */
    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone( "UTC" );
    
    private static final ThreadLocalFormat DATE = new ThreadLocalFormat( DATE_FORMAT ),
            TIME = new ThreadLocalFormat( TIME_FORMAT );

    private static class ThreadLocalFormat extends ThreadLocal<DateFormat>
    {
        private final String format;

        ThreadLocalFormat( String format )
        {
            this.format = format;
        }

        String format( Date date, TimeZone timeZone )
        {
            DateFormat dateFormat = get();
            dateFormat.setTimeZone( timeZone );
            return dateFormat.format( date );
        }

        @Override
        protected DateFormat initialValue()
        {
            return new SimpleDateFormat( format );
        }
    }
}
