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

import java.time.LocalTime;
import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

class LocalTimeType extends Type
{
    // Affected key state:
    // long0 (nanoOfDay)

    LocalTimeType( byte typeId )
    {
        super( ValueGroup.LOCAL_TIME, typeId, LocalTimeValue.MIN_VALUE, LocalTimeValue.MAX_VALUE );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return GenericKey.SIZE_LOCAL_TIME;
    }

    @Override
    void copyValue( GenericKey to, GenericKey from )
    {
        to.long0 = from.long0;
    }

    @Override
    Value asValue( GenericKey state )
    {
        return asValue( state.long0 );
    }

    @Override
    int compareValue( GenericKey left, GenericKey right )
    {
        return compare(
                left.long0,
                right.long0 );
    }

    @Override
    void putValue( PageCursor cursor, GenericKey state )
    {
        put( cursor, state.long0 );
    }

    @Override
    boolean readValue( PageCursor cursor, int size, GenericKey into )
    {
        return read( cursor, into );
    }

    static LocalTimeValue asValue( long long0 )
    {
        return LocalTimeValue.localTime( asValueRaw( long0 ) );
    }

    static LocalTime asValueRaw( long long0 )
    {
        return LocalTimeValue.localTimeRaw( long0 );
    }

    static int compare( long this_long0, long that_long0 )
    {
        return Long.compare( this_long0, that_long0 );
    }

    static void put( PageCursor cursor, long long0 )
    {
        cursor.putLong( long0 );
    }

    static boolean read( PageCursor cursor, GenericKey into )
    {
        into.writeLocalTime( cursor.getLong() );
        return true;
    }

    void write( GenericKey state, long nanoOfDay )
    {
        state.long0 = nanoOfDay;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0=" + state.long0 );
    }
}
