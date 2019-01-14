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

import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.impl.index.schema.DurationIndexKey.AVG_DAY_SECONDS;
import static org.neo4j.kernel.impl.index.schema.DurationIndexKey.AVG_MONTH_SECONDS;

class DurationType extends Type
{
    // Affected key state:
    // long0 (totalAvgSeconds)
    // long1 (nanosOfSecond)
    // long2 (months)
    // long3 (days)

    DurationType( byte typeId )
    {
        super( ValueGroup.DURATION, typeId, DurationValue.MIN_VALUE, DurationValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return GenericKey.SIZE_DURATION;
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

    static DurationValue asValue( long long0, long long1, long long2, long long3  )
    {
        // DurationValue has no "raw" variant
        long seconds = long0 - long2 * AVG_MONTH_SECONDS - long3 * AVG_DAY_SECONDS;
        return DurationValue.duration( long2, long3, seconds, long1 );
    }

    static int compare(
            long this_long0, long this_long1, long this_long2, long this_long3,
            long that_long0, long that_long1, long that_long2, long that_long3 )
    {
        int comparison = Long.compare( this_long0, that_long0 );
        if ( comparison == 0 )
        {
            comparison = Integer.compare( (int) this_long1, (int) that_long1 );
            if ( comparison == 0 )
            {
                comparison = Long.compare( this_long2, that_long2 );
                if ( comparison == 0 )
                {
                    comparison = Long.compare( this_long3, that_long3 );
                }
            }
        }
        return comparison;
    }

    static void put( PageCursor cursor, long long0, long long1, long long2, long long3 )
    {
        cursor.putLong( long0 );
        cursor.putInt( (int) long1 );
        cursor.putLong( long2 );
        cursor.putLong( long3 );
    }

    static boolean read( PageCursor cursor, GenericKey into )
    {
        // TODO unify order of fields
        long totalAvgSeconds = cursor.getLong();
        int nanosOfSecond = cursor.getInt();
        long months = cursor.getLong();
        long days = cursor.getLong();
        into.writeDurationWithTotalAvgSeconds( months, days, totalAvgSeconds, nanosOfSecond );
        return true;
    }

    void write( GenericKey state, long months, long days, long totalAvgSeconds, int nanos )
    {
        state.long0 = totalAvgSeconds;
        state.long1 = nanos;
        state.long2 = months;
        state.long3 = days;
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
