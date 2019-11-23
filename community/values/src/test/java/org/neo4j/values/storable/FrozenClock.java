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

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalField;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class FrozenClock extends Clock implements Supplier<ZoneId>
{
    private Instant instant;
    private ZoneId zone;

    FrozenClock( String zoneId )
    {
        instant = Instant.now();
        zone = ZoneId.of(zoneId);
    }

    @Override
    public ZoneId getZone()
    {
        return zone;
    }

    Clock withZone( String zoneId )
    {
        return withZone( ZoneId.of( zoneId ) );
    }

    @Override
    public Clock withZone( ZoneId zone )
    {
        return fixed( instant, zone );
    }

    @Override
    public Instant instant()
    {
        return instant;
    }

    @Override
    public long millis()
    {
        return instant.toEpochMilli();
    }

    public Clock at( LocalDateTime datetime )
    {
        return at( datetime.atZone( zone ) );
    }

    public Clock at( OffsetDateTime datetime )
    {
        return fixed( datetime.toInstant(), datetime.getOffset() );
    }

    public Clock at( ZonedDateTime datetime )
    {
        return fixed( datetime.toInstant(), datetime.getZone() );
    }

    public Clock with( TemporalAdjuster adjuster )
    {
        return fixed( instant.with( adjuster ), zone );
    }

    public Clock with( TemporalField field, long newValue )
    {
        return fixed( instant.with( field, newValue ), zone );
    }

    @Override
    public ZoneId get()
    {
        return zone;
    }

    static <V extends TemporalValue<?,V>> void assertEqualTemporal( V expected, V actual )
    {
        assertThat( actual ).isEqualTo( expected );
        assertEqualsWithMapping( actual, expected, FrozenClock::timezone );
        assertEqualsWithMapping( actual, expected, TemporalValue::temporal );
    }

    private static ZoneId timezone( TemporalValue<?,?> temporal )
    {
        if ( temporal instanceof DateTimeValue )
        {
            return ((DateTimeValue) temporal).temporal().getZone();
        }
        if ( temporal instanceof TimeValue )
        {
            return ((TimeValue) temporal).temporal().getOffset();
        }
        return null;
    }

    private static <T, U> void assertEqualsWithMapping( T actual, T expected, Function<T,U> mapping )
    {
        assertThat( mapping.apply( expected ) ).isEqualTo( mapping.apply( actual ) );
    }
}
