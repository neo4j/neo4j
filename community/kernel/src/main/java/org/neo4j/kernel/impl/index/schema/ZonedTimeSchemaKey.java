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
package org.neo4j.kernel.impl.index.schema;

import java.time.ZoneOffset;

import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Includes value and entity id (to be able to handle non-unique values). A value can be any {@link TimeValue}.
 *
 * With these keys the TimeValues are sorted by UTC time of day, and then by time zone.
 */
class ZonedTimeSchemaKey extends NativeSchemaKey<ZonedTimeSchemaKey>
{
    static final int SIZE =
            Long.BYTES +    /* nanosOfDayUTC */
            Integer.BYTES + /* zoneOffsetSeconds */
            Long.BYTES;     /* entityId */

    long nanosOfDayUTC;
    int zoneOffsetSeconds;

    @Override
    public Value asValue()
    {
        // We need to check validity upfront without throwing exceptions, because the PageCursor might give garbage bytes
        if ( TimeZones.validZoneOffset( zoneOffsetSeconds ) )
        {
            return TimeValue.time( nanosOfDayUTC, ZoneOffset.ofTotalSeconds( zoneOffsetSeconds ) );
        }
        return NO_VALUE;
    }

    @Override
    public void initValueAsLowest()
    {
        nanosOfDayUTC = Long.MIN_VALUE;
        zoneOffsetSeconds = Integer.MIN_VALUE;
    }

    @Override
    public void initValueAsHighest()
    {
        nanosOfDayUTC = Long.MAX_VALUE;
        zoneOffsetSeconds = Integer.MAX_VALUE;
    }

    @Override
    public int compareValueTo( ZonedTimeSchemaKey other )
    {
        int compare = Long.compare( nanosOfDayUTC, other.nanosOfDayUTC );
        if ( compare == 0 )
        {
            compare = Integer.compare( zoneOffsetSeconds, other.zoneOffsetSeconds );
        }
        return compare;
    }

    @Override
    public String toString()
    {
        return format( "value=%s,entityId=%d,nanosOfDayUTC=%d,zoneOffsetSeconds=%d",
                        asValue(), getEntityId(), nanosOfDayUTC, zoneOffsetSeconds );
    }

    @Override
    public void writeTime( long nanosOfDayUTC, int offsetSeconds )
    {
        this.nanosOfDayUTC = nanosOfDayUTC;
        this.zoneOffsetSeconds = offsetSeconds;
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( !(value instanceof TimeValue) )
        {
            throw new IllegalArgumentException(
                    "Key layout does only support TimeValue, tried to create key from " + value );
        }
        return value;
    }
}
