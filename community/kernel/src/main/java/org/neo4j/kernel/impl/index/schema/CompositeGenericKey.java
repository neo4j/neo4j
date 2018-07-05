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

class CompositeGenericKey extends NativeIndexKey<CompositeGenericKey>
{
    // TODO we have multiple places defining size of the entityId!!
    private static final int ENTITY_ID_SIZE = Long.BYTES;

    private GenericKeyState[] states;

    CompositeGenericKey( int length )
    {
        states = new GenericKeyState[length];
        for ( int i = 0; i < states.length; i++ )
        {
            states[i] = new GenericKeyState();
        }
    }

    @Override
    void writeValue( int stateSlot, Value value, Inclusion inclusion )
    {
        states[stateSlot].writeValue( value, inclusion );
    }

    @Override
    void assertValidValue( int stateSlot, Value value )
    {
        states[stateSlot].assertCorrectType( value );
    }

    @Override
    void initialize( long entityId )
    {
        super.initialize( entityId );
        for ( GenericKeyState state : states )
        {
            state.clear();
        }
    }

    @Override
    String propertiesAsString()
    {
        return "todo";
    }

    @Override
    Value[] asValues()
    {
        Value[] values = new Value[states.length];
        for ( int i = 0; i < states.length; i++ )
        {
            values[i] = states[i].asValue();
        }
        return values;
    }

    @Override
    void initValueAsLowest( int stateSlot, ValueGroup valueGroup )
    {
        states[stateSlot].initValueAsLowest( valueGroup );
    }

    @Override
    void initValueAsHighest( int stateSlot, ValueGroup valueGroup )
    {
        states[stateSlot].initValueAsHighest( valueGroup );
    }

    @Override
    int compareValueTo( CompositeGenericKey other )
    {
        for ( int i = 0; i < states.length; i++ )
        {
            int comparison = states[i].compareValueTo( other.states[i] );
            if ( comparison != 0 )
            {
                return comparison;
            }
        }
        return 0;
    }

    void initAsPrefixLow( int stateSlot, String prefix )
    {
        states[stateSlot].initAsPrefixLow( prefix );
    }

    void initAsPrefixHigh( int stateSlot, String prefix )
    {
        states[stateSlot].initAsPrefixHigh( prefix );
    }

    void copyValuesFrom( CompositeGenericKey key )
    {
        if ( key.states.length != states.length )
        {
            throw new IllegalArgumentException( "Different state lengths " + key.states.length + " vs " + states.length );
        }

        for ( int i = 0; i < key.states.length; i++ )
        {
            states[i].copyFrom( key.states[i] );
        }
    }

    int size()
    {
        int size = ENTITY_ID_SIZE;
        for ( GenericKeyState state : states )
        {
            size += state.size();
        }
        return size;
    }

    void write( PageCursor cursor )
    {
        cursor.putLong( getEntityId() );
        for ( GenericKeyState state : states )
        {
            state.put( cursor );
        }
    }

    void read( PageCursor cursor, int keySize )
    {
        if ( keySize < ENTITY_ID_SIZE )
        {
            initializeToDummyValue();
            return;
        }

        setEntityId( cursor.getLong() );
        int offset = cursor.getOffset();
        for ( GenericKeyState state : states )
        {
            if ( !state.read( cursor, keySize ) )
            {
                initializeToDummyValue();
                return;
            }
            int offsetAfterRead = cursor.getOffset();
            keySize -= offsetAfterRead - offset;
            offset = offsetAfterRead;
        }
    }

    private void initializeToDummyValue()
    {
        setEntityId( Long.MIN_VALUE );
        for ( GenericKeyState state : states )
        {
            state.initializeToDummyValue();
        }
    }

    @Override
    int numberOfStateSlots()
    {
        return states.length;
    }
}
