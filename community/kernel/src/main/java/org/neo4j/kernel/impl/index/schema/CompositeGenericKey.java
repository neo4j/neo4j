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

import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

/**
 * {@link GenericKey} which has an array of {@link GenericKey} inside and can therefore hold composite key state.
 * For single-keys please instead use the more efficient {@link GenericKey}.
 */
class CompositeGenericKey extends GenericKey
{
    @SuppressWarnings( { "unchecked", "rawtypes" } )
    private GenericKey[] states;

    CompositeGenericKey( int slots, IndexSpecificSpaceFillingCurveSettingsCache spatialSettings )
    {
        super( spatialSettings );
        states = new GenericKey[slots];
        for ( int i = 0; i < states.length; i++ )
        {
            states[i] = new GenericKey( spatialSettings );
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
    int compareValueTo( GenericKey other )
    {
        for ( int i = 0; i < states.length; i++ )
        {
            int comparison = states[i].compareValueToInternal( other.stateSlot( i ) );
            if ( comparison != 0 )
            {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    void copyFrom( GenericKey key )
    {
        if ( key.numberOfStateSlots() != states.length )
        {
            throw new IllegalArgumentException( "Different state lengths " + key.numberOfStateSlots() + " vs " + states.length );
        }

        for ( int i = 0; i < states.length; i++ )
        {
            states[i].copyFromInternal( key.stateSlot( i ) );
        }
    }

    @Override
    int size()
    {
        int size = ENTITY_ID_SIZE;
        for ( GenericKey state : states )
        {
            size += state.sizeInternal();
        }
        return size;
    }

    @Override
    void put( PageCursor cursor )
    {
        cursor.putLong( getEntityId() );
        for ( GenericKey state : states )
        {
            state.putInternal( cursor );
        }
    }

    @Override
    boolean get( PageCursor cursor, int keySize )
    {
        int offset = cursor.getOffset();
        for ( GenericKey state : states )
        {
            if ( !state.getInternal( cursor, keySize ) )
            {
                initializeToDummyValue();
                return false;
            }
            int offsetAfterRead = cursor.getOffset();
            keySize -= offsetAfterRead - offset;
            offset = offsetAfterRead;
        }
        return true;
    }

    @Override
    void initializeToDummyValue()
    {
        setEntityId( Long.MIN_VALUE );
        for ( GenericKey state : states )
        {
            state.initializeToDummyValueInternal();
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
        for ( GenericKey state : states )
        {
            joiner.add( state.toStringInternal() );
        }
        return joiner.toString();
    }

    @Override
    void minimalSplitter( GenericKey left, GenericKey right, GenericKey into )
    {
        int firstStateToDiffer = 0;
        int compare = 0;
        int stateCount = right.numberOfStateSlots();

        // It's really quite assumed that all these keys have the same number of state slots.
        // It's not a practical runtime concern, so merely an assertion here
        assert right.numberOfStateSlots() == stateCount;
        assert into.numberOfStateSlots() == stateCount;

        while ( compare == 0 && firstStateToDiffer < stateCount )
        {
            GenericKey leftState = left.stateSlot( firstStateToDiffer );
            GenericKey rightState = right.stateSlot( firstStateToDiffer );
            firstStateToDiffer++;
            compare = leftState.compareValueToInternal( rightState );
        }
        firstStateToDiffer--; // Rewind last increment
        for ( int i = 0; i < firstStateToDiffer; i++ )
        {
            into.stateSlot( i ).copyFromInternal( right.stateSlot( i ) );
        }
        for ( int i = firstStateToDiffer; i < stateCount; i++ )
        {
            GenericKey leftState = left.stateSlot( i );
            GenericKey rightState = right.stateSlot( i );
            rightState.minimalSplitterInternal( leftState, rightState, into.stateSlot( i ) );
        }
    }

    @Override
    GenericKey stateSlot( int slot )
    {
        return states[slot];
    }
}
