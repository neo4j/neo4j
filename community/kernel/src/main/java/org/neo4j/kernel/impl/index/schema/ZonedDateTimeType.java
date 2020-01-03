/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.ZONE_ID_FLAG;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.ZONE_ID_MASK;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.asZoneId;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.asZoneOffset;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.isZoneId;

class ZonedDateTimeType extends Type
{
    // Affected key state:
    // long0 (epochSecondUTC)
    // long1 (nanoOfSecond)
    // long2 (zoneId)
    // long3 (zoneOffsetSeconds)

    ZonedDateTimeType( byte typeId )
    {
        super( ValueGroup.ZONED_DATE_TIME, typeId, DateTimeValue.MIN_VALUE, DateTimeValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return GenericKey.SIZE_ZONED_DATE_TIME;
    }

    @Override
    void copyValue( GenericKey to, GenericKey from )
    {
        to.long0 = from.long0;
        to.long1 = from.long1;
        to.long2 = from.long2;
        to.long3 = from.long3;
    }

    @Override
    Value asValue( GenericKey state )
    {
        return asValue( state.long0, state.long1, state.long2, state.long3 );
    }

    @Override
    int compareValue( GenericKey left, GenericKey right )
    {
        return compare(
                left.long0, left.long1, left.long2, left.long3,
                right.long0, right.long1, right.long2, right.long3 );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey state )
    {
        put( cursor, state.long0, state.long1, state.long2, state.long3 );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey into )
    {
        return read( cursor, into );
    }

    static int compare(
            long this_long0, long this_long1, long this_long2, long this_long3,
            long that_long0, long that_long1, long that_long2, long that_long3 )
    {
        int compare = Long.compare( this_long0, that_long0 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) this_long1, (int) that_long1 );
            if ( compare == 0 &&
                    // We need to check validity upfront without throwing exceptions, because the PageCursor might give garbage bytes
                    TimeZones.validZoneOffset( (int) this_long3 ) &&
                    TimeZones.validZoneOffset( (int) that_long3 ) )
            {
                // In the rare case of comparing the same instant in different time zones, we settle for
                // mapping to values and comparing using the general values comparator.
                compare = Values.COMPARATOR.compare(
                        asValue( this_long0, this_long1, this_long2, this_long3 ),
                        asValue( that_long0, that_long1, that_long2, that_long3 ) );
            }
        }
        return compare;
    }

    static void put( PageCursor cursor, long long0, long long1, long long2, long long3 )
    {
        cursor.putLong( long0 );
        cursor.putInt( (int) long1 );
        if ( long2 >= 0 )
        {
            cursor.putInt( (int) long2 | ZONE_ID_FLAG );
        }
        else
        {
            cursor.putInt( (int) long3 & ZONE_ID_MASK );
        }
    }

    static boolean read( PageCursor cursor, GenericKey into )
    {
        long epochSecondUTC = cursor.getLong();
        int nanoOfSecond = cursor.getInt();
        int encodedZone = cursor.getInt();
        if ( isZoneId( encodedZone ) )
        {
            into.writeDateTime( epochSecondUTC, nanoOfSecond, asZoneId( encodedZone ) );
        }
        else
        {
            into.writeDateTime( epochSecondUTC, nanoOfSecond, asZoneOffset( encodedZone ) );
        }
        return true;
    }

    static DateTimeValue asValue( long long0, long long1, long long2, long long3 )
    {
        return DateTimeValue.datetime( asValueRaw( long0, long1, long2, long3 ) );
    }

    static ZonedDateTime asValueRaw( long long0, long long1, long long2, long long3 )
    {
        return TimeZones.validZoneId( (short) long2 ) ?
               DateTimeValue.datetimeRaw( long0, long1, ZoneId.of( TimeZones.map( (short) long2 ) ) ) :
               DateTimeValue.datetimeRaw( long0, long1, ZoneOffset.ofTotalSeconds( (int) long3 ) );
    }

    void write( GenericKey state, long epochSecondUTC, int nano, short zoneId, int offsetSeconds )
    {
        state.long0 = epochSecondUTC;
        state.long1 = nano;
        state.long2 = zoneId;
        state.long3 = offsetSeconds;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0=" + state.long0 );
        joiner.add( "long1=" + state.long1 );
        joiner.add( "long2=" + state.long2 );
        joiner.add( "long3=" + state.long3 );
    }
}
