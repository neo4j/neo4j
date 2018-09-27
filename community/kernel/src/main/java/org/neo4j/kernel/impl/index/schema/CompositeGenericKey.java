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

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

/**
 * {@link GenericKey} which has an array of {@link GenericKeyState} inside and can therefore hold composite key state.
 * For single-keys please instead use the more efficient {@link SingleGenericKey}.
 */
class CompositeGenericKey extends GenericKey
{
    private GenericKeyState[] states;

    CompositeGenericKey( int slots, IndexSpecificSpaceFillingCurveSettingsCache spatialSettings )
    {
        super( spatialSettings );
        states = new GenericKeyState[slots];
        for ( int i = 0; i < states.length; i++ )
        {
            states[i] = new GenericKeyState( spatialSettings );
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

    /**
     * Special init method for when doing sub-queries for geometry range. This given slot will not be initialized with a {@link PointValue},
     * but a 1D value which is derived from that point, calculated based on intersecting tiles.
     *
     * @see SpaceFillingCurve#getTilesIntersectingEnvelope(double[], double[], SpaceFillingCurveConfiguration)
     */
    void initFromDerivedSpatialValue( int stateSlot, CoordinateReferenceSystem crs, long derivedValue, Inclusion inclusion )
    {
        states[stateSlot].writePointDerived( crs, derivedValue, inclusion );
    }

    void initAsPrefixLow( int stateSlot, String prefix )
    {
        states[stateSlot].initAsPrefixLow( prefix );
    }

    void initAsPrefixHigh( int stateSlot, String prefix )
    {
        states[stateSlot].initAsPrefixHigh( prefix );
    }

    @Override
    int compareValueTo( GenericKey other )
    {
        for ( int i = 0; i < states.length; i++ )
        {
            int comparison = states[i].compareValueTo( other.stateSlot( i ) );
            if ( comparison != 0 )
            {
                return comparison;
            }
        }
        return 0;
    }

    @Override
    void copyValuesFrom( GenericKey key )
    {
        if ( key.numberOfStateSlots() != states.length )
        {
            throw new IllegalArgumentException( "Different state lengths " + key.numberOfStateSlots() + " vs " + states.length );
        }

        for ( int i = 0; i < states.length; i++ )
        {
            states[i].copyFrom( key.stateSlot( i ) );
        }
    }

    @Override
    int size()
    {
        int size = ENTITY_ID_SIZE;
        for ( GenericKeyState state : states )
        {
            size += state.stateSize();
        }
        return size;
    }

    @Override
    void write( PageCursor cursor )
    {
        for ( GenericKeyState state : states )
        {
            state.put( cursor );
        }
    }

    @Override
    boolean read( PageCursor cursor, int keySize )
    {
        int offset = cursor.getOffset();
        for ( GenericKeyState state : states )
        {
            if ( !state.get( cursor, keySize ) )
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
            GenericKeyState leftState = left.stateSlot( firstStateToDiffer );
            GenericKeyState rightState = right.stateSlot( firstStateToDiffer );
            firstStateToDiffer++;
            compare = leftState.compareValueTo( rightState );
        }
        firstStateToDiffer--; // Rewind last increment
        for ( int i = 0; i < firstStateToDiffer; i++ )
        {
            into.stateSlot( i ).copyFrom( right.stateSlot( i ) );
        }
        for ( int i = firstStateToDiffer; i < stateCount; i++ )
        {
            GenericKeyState.minimalSplitter( left.stateSlot( i ), right.stateSlot( i ), into.stateSlot( i ) );
        }
        into.setCompareId( right.getCompareId() );
        into.setEntityId( right.getEntityId() );
    }

    @Override
    GenericKeyState stateSlot( int slot )
    {
        return states[slot];
    }
}
