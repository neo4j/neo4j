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

import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.values.storable.Values.NO_VALUE;

class ZonedTimeType extends Type
{
    // Affected key state:
    // long0 (nanosOfDayUTC)
    // long1 (zoneOffsetSeconds)

    ZonedTimeType( byte typeId )
    {
        super( ValueGroup.ZONED_TIME, typeId, TimeValue.MIN_VALUE, TimeValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return GenericKey.SIZE_ZONED_TIME;
    }

    @Override
    void copyValue( GenericKey to, GenericKey from )
    {
        to.long0 = from.long0;
        to.long1 = from.long1;
    }

    @Override
    Value asValue( GenericKey state )
    {
        return asValue( state.long0, state.long1 );
    }

    @Override
    int compareValue( GenericKey left, GenericKey right )
    {
        return compare(
                left.long0, left.long1,
                right.long0, right.long1 );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey state )
    {
        put( cursor, state.long0, state.long1 );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey into )
    {
        return read( cursor, into );
    }

    static Value asValue( long long0, long long1 )
    {
        OffsetTime time = asValueRaw( long0, long1 );
        return time != null ? TimeValue.time( time ) : NO_VALUE;
    }

    static OffsetTime asValueRaw( long long0, long long1 )
    {
        if ( TimeZones.validZoneOffset( (int) long1 ) )
        {
            return TimeValue.timeRaw( long0, ZoneOffset.ofTotalSeconds( (int) long1 ) );
        }
        // TODO Getting here means that after a proper read this value is plain wrong... shouldn't something be thrown instead? Yes and same for TimeZones
        return null;
    }

    static void put( PageCursor cursor, long long0, long long1 )
    {
        cursor.putLong( long0 );
        cursor.putInt( (int) long1 );
    }

    static boolean read( PageCursor cursor, GenericKey into )
    {
        into.writeTime( cursor.getLong(), cursor.getInt() );
        return true;
    }

    static int compare(
            long this_long0, long this_long1,
            long that_long0, long that_long1 )
    {
        int compare = Long.compare( this_long0, that_long0 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) this_long1, (int) that_long1 );
        }
        return compare;
    }

    void write( GenericKey state, long nanosOfDayUTC, int offsetSeconds )
    {
        state.long0 = nanosOfDayUTC;
        state.long1 = offsetSeconds;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0=" + state.long0 );
        joiner.add( "long1=" + state.long1 );
    }
}
