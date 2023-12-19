/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ProceduresTimeFormatHelper
{
    static String formatTime( final long startTime, ZoneId zoneId )
    {
        return OffsetDateTime
                .ofInstant( Instant.ofEpochMilli( startTime ), zoneId )
                .format( ISO_OFFSET_DATE_TIME );
    }

    static String formatInterval( final long l )
    {
        final long hr = MILLISECONDS.toHours( l );
        final long min = MILLISECONDS.toMinutes( l - HOURS.toMillis( hr ) );
        final long sec = MILLISECONDS.toSeconds( l - HOURS.toMillis( hr ) - MINUTES.toMillis( min ) );
        final long ms = l - HOURS.toMillis( hr ) - MINUTES.toMillis( min ) - SECONDS.toMillis( sec );
        return String.format( "%02d:%02d:%02d.%03d", hr, min, sec, ms );
    }

}
