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

import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;

class CompositeGenericKey extends NativeIndexKey<CompositeGenericKey>
{
    private GenericKeyState[] states;

    CompositeGenericKey( int slots )
    {
        states = new GenericKeyState[slots];
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
        Preconditions.requireBetween( stateSlot, 0, states.length );
        GenericKeyState.assertCorrectType( value );
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
        return Arrays.toString( asValues() );
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
            initializeToDummyValue( cursor );
            cursor.setCursorException( format( "Failed to read CompositeGenericKey due to keySize < ENTITY_ID_SIZE, more precisely %d", keySize ) );
            return;
        }

        initialize( cursor.getLong() );
        int offset = cursor.getOffset();
        int stateOffset = 0;
        for ( GenericKeyState state : states )
        {
            if ( !state.read( cursor, keySize ) )
            {
                initializeToDummyValue( cursor );
                return;
            }
            int offsetAfterRead = cursor.getOffset();
            keySize -= offsetAfterRead - offset;
            offset = offsetAfterRead;
            stateOffset++;
        }
    }

    private void initializeToDummyValue( PageCursor cursor )
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

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ",", "[", "]" );
        for ( GenericKeyState state : states )
        {
            joiner.add( state.toString() );
        }
        return joiner.toString();
    }

    public static void minimalSplitter( CompositeGenericKey left, CompositeGenericKey right, CompositeGenericKey into )
    {
        int firstStateToDiffer = 0;
        int compare = 0;
        while ( compare == 0 && firstStateToDiffer < right.states.length )
        {
            GenericKeyState leftState = left.states[firstStateToDiffer];
            GenericKeyState rightState = right.states[firstStateToDiffer];
            firstStateToDiffer++;
            compare = leftState.compareValueTo( rightState );
        }
        firstStateToDiffer--; // Rewind last increment
        for ( int i = 0; i < firstStateToDiffer; i++ )
        {
            into.states[i].copyFrom( right.states[i] );
        }
        GenericKeyState.minimalSplitter( left.states[firstStateToDiffer], right.states[firstStateToDiffer], into.states[firstStateToDiffer] );
        for ( int i = firstStateToDiffer + 1; i < into.states.length; i++ )
        {
            into.states[i].initializeToDummyValue();
        }
        into.setCompareId( right.getCompareId() );
        into.setEntityId( right.getEntityId() );
    }
}
