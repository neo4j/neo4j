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

import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.GenericKey.FALSE;
import static org.neo4j.kernel.impl.index.schema.GenericKey.TRUE;

class BooleanType extends Type
{
    // Affected key state:
    // long0

    BooleanType( byte typeId )
    {
        super( ValueGroup.BOOLEAN, typeId, Values.of( false ), Values.of( true ) );
    }

    @Override
    int valueSize( GenericKey state )
    {
        return GenericKey.SIZE_BOOLEAN;
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

    static BooleanValue asValue( long long0 )
    {
        return Values.booleanValue( asValueRaw( long0 ) );
    }

    static boolean asValueRaw( long long0 )
    {
        return booleanOf( long0 );
    }

    static int compare(
            long this_long0,
            long that_long0 )
    {
        return Long.compare( this_long0, that_long0 );
    }

    static void put( PageCursor cursor, long long0 )
    {
        cursor.putByte( (byte) long0 );
    }

    static boolean read( PageCursor cursor, GenericKey into )
    {
        into.writeBoolean( cursor.getByte() == TRUE );
        return true;
    }

    void write( GenericKey state, boolean value )
    {
        state.long0 = value ? TRUE : FALSE;
    }

    @Override
    protected void addTypeSpecificDetails( StringJoiner joiner, GenericKey state )
    {
        joiner.add( "long0=" + state.long0 );
    }
}
