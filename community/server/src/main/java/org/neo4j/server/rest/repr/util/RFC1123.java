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
package org.neo4j.server.rest.repr.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public final class RFC1123
{
    private static final ThreadLocal<RFC1123> INSTANCES = new ThreadLocal<RFC1123>();

    public static final TimeZone GMT = TimeZone.getTimeZone( "GMT" );

    private static final Date Y2K_START_DATE;

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone( GMT );
        calendar.set( 2000, Calendar.JANUARY, 1, 0, 0, 0 );
        calendar.set( Calendar.MILLISECOND, 0 );
        Y2K_START_DATE = calendar.getTime();
    }

    private final SimpleDateFormat format;

    private RFC1123()
    {
        format = new SimpleDateFormat( "EEE, dd MMM yyyy HH:mm:ss Z", Locale.US );
        format.setTimeZone( GMT );
    }

    public Date parse(String input) throws ParseException
    {
        format.set2DigitYearStart( Y2K_START_DATE );
        return format.parse( input );
    }

    public String format(Date date)
    {
        if ( null == date )
            throw new IllegalArgumentException( "Date is null" );

        return format.format( date );
    }

    static final RFC1123 instance()
    {
        RFC1123 instance = INSTANCES.get();
        if ( null == instance )
        {
            instance = new RFC1123();
            INSTANCES.set( instance );
        }
        return instance;
    }

    public static Date parseTimestamp(String input) throws ParseException
    {
        return instance().parse( input );
    }

    public static String formatDate(Date date)
    {
        return instance().format( date );
    }
}
