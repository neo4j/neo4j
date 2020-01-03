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

import java.time.LocalDateTime;
import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

class LocalDateTimeType extends Type
{
    // Affected key state:
    // long0 (nanoOfSecond)
    // long1 (epochSecond)

    LocalDateTimeType( byte typeId )
    {
        super( ValueGroup.LOCAL_DATE_TIME, typeId, LocalDateTimeValue.MIN_VALUE, LocalDateTimeValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return GenericKey.SIZE_LOCAL_DATE_TIME;
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

    static LocalDateTimeValue asValue( long long0, long long1 )
    {
        return LocalDateTimeValue.localDateTime( asValueRaw( long0, long1 ) );
    }

    static LocalDateTime asValueRaw( long long0, long long1 )
    {
        return LocalDateTimeValue.localDateTimeRaw( long1, long0 );
    }

    static void put( PageCursor cursor, long long0, long long1 )
    {
        cursor.putLong( long1 );
        cursor.putInt( (int) long0 );
    }

    static boolean read( PageCursor cursor, GenericKey into )
    {
        into.writeLocalDateTime( cursor.getLong(), cursor.getInt() );
        return true;
    }

    static int compare(
            long this_long0, long this_long1,
            long that_long0, long that_long1 )
    {
        int compare = Long.compare( this_long1, that_long1 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) this_long0, (int) that_long0 );
        }
        return compare;
    }

    void write( GenericKey state, long epochSecond, int nano )
    {
        state.long0 = nano;
        state.long1 = epochSecond;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0=" + state.long0 );
        joiner.add( "long1=" + state.long1 );
    }
}
