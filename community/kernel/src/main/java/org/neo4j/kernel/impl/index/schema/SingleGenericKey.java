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
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

/**
 * {@link GenericKey} which can handle single-keys and operates on its {@link GenericKeyState} inside it, since it actually has
 * that state inside of it.
 * For composite keys please use {@link CompositeGenericKey}.
 */
class SingleGenericKey extends GenericKey
{
    SingleGenericKey( IndexSpecificSpaceFillingCurveSettingsCache settings )
    {
        super( settings );
    }

    @Override
    void writeValue( int stateSlot, Value value, Inclusion inclusion )
    {
        writeValue( value, inclusion );
    }

    @Override
    void initialize( long entityId )
    {
        super.initialize( entityId );
        clear();
    }

    @Override
    void initFromDerivedSpatialValue( int stateSlot, CoordinateReferenceSystem crs, long derivedValue, Inclusion inclusion )
    {
        writePointDerived( crs, derivedValue, inclusion );
    }

    @Override
    void initAsPrefixLow( int stateSlot, String prefix )
    {
        initAsPrefixLow( prefix );
    }

    @Override
    void initAsPrefixHigh( int stateSlot, String prefix )
    {
        initAsPrefixHigh( prefix );
    }

    @Override
    void copyValuesFrom( GenericKey key )
    {
        copyFrom( key );
    }

    @Override
    void write( PageCursor cursor )
    {
        put( cursor );
    }

    @Override
    int size()
    {
        return ENTITY_ID_SIZE + stateSize();
    }

    @Override
    boolean read( PageCursor cursor, int keySize )
    {
        return get( cursor, keySize );
    }

    @Override
    void minimalSplitter( GenericKey left, GenericKey right, GenericKey into )
    {
        GenericKeyState.minimalSplitter( left, right, into );
        into.setCompareId( right.getCompareId() );
        into.setEntityId( right.getEntityId() );
    }

    @Override
    GenericKeyState stateSlot( int slot )
    {
        assert slot == 0;
        return this;
    }

    @Override
    Value[] asValues()
    {
        return new Value[] {asValue()};
    }

    @Override
    void initValueAsLowest( int stateSlot, ValueGroup valueGroup )
    {
        initValueAsLowest( valueGroup );
    }

    @Override
    void initValueAsHighest( int stateSlot, ValueGroup valueGroup )
    {
        initValueAsHighest( valueGroup );
    }

    @Override
    int numberOfStateSlots()
    {
        return 1;
    }

    @Override
    int compareValueTo( GenericKey other )
    {
        return compareValueTo( (GenericKeyState) other );
    }
}
