/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class CompositeGenericKey extends NativeIndexKey<CompositeGenericKey>
{
    // TODO we have multiple places defining size of the entityId!!
    private static final int ENTITY_ID_SIZE = Long.BYTES;

    GenericKeyState state;

    @Override
    protected void writeValues( Value[] values )
    {
        values[0].writeTo( state );
    }

    @Override
    protected void assertValidValues( Value[] values )
    {
        if ( values.length > 1 )
        {
            throw new IllegalArgumentException( "Composite values" );
        }
        state.assertCorrectType( values[0] );
    }

    @Override
    void initialize( long entityId )
    {
        super.initialize( entityId );
        state.clear();
    }

    @Override
    protected String propertiesAsString()
    {
        return null;
    }

    Value[] asValues()
    {
        return new Value[] {state.asValue()};
    }

    @Override
    void initValueAsLowest( ValueGroup... valueGroups )
    {
        state.initValueAsLowest( valueGroups[0] );
    }

    @Override
    void initValueAsHighest( ValueGroup... valueGroups )
    {
        state.initValueAsHighest( valueGroups[0] );
    }

    @Override
    int compareValueTo( CompositeGenericKey other )
    {
        return state.compareValueTo( other.state );
    }

    void initAsPrefixLow( String prefix )
    {
        initialize( Long.MIN_VALUE );
        state.initAsPrefixLow( prefix );
    }

    void initAsPrefixHigh( String prefix )
    {
        initialize( Long.MAX_VALUE );
        state.initAsPrefixHigh( prefix );
    }

    void copyValuesFrom( CompositeGenericKey key )
    {
        state.copyFrom( key.state );
    }

    int size()
    {
        int size = ENTITY_ID_SIZE;
        size += state.size();
        return size;
    }

    void write( PageCursor cursor )
    {
        cursor.putLong( getEntityId() );
        state.write( cursor );
    }

    void read( PageCursor cursor, int keySize )
    {
        if ( keySize < ENTITY_ID_SIZE )
        {
            initializeToDummyValue();
            return;
        }

        setEntityId( cursor.getLong() );
        state.read( cursor, keySize - ENTITY_ID_SIZE );
    }

    private void initializeToDummyValue()
    {
        setEntityId( Long.MIN_VALUE );
        state.initializeToDummyValue();
    }
}
