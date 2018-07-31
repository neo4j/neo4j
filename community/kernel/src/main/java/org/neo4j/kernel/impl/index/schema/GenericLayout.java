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
import java.util.Collections;
import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.ValueGroup;

import static java.util.Comparator.comparing;

class GenericLayout extends IndexLayout<CompositeGenericKey,NativeIndexValue>
{
    static final Comparator<Type> TYPE_COMPARATOR = comparing( t -> t.valueGroup );
    private final int numberOfSlots;

    // Order doesn't matter since it's each Type's ValueGroup that matters and will be used for comparison
    enum Type
    {
        ZONED_DATE_TIME( ValueGroup.ZONED_DATE_TIME, (byte) 0 ),
        LOCAL_DATE_TIME( ValueGroup.LOCAL_DATE_TIME, (byte) 1 ),
        DATE( ValueGroup.DATE, (byte) 2 ),
        ZONED_TIME( ValueGroup.ZONED_TIME, (byte) 3 ),
        LOCAL_TIME( ValueGroup.LOCAL_TIME, (byte) 4 ),
        DURATION( ValueGroup.DURATION, (byte) 5 ),
        TEXT( ValueGroup.TEXT, (byte) 6 ),
        BOOLEAN( ValueGroup.BOOLEAN, (byte) 7 ),
        NUMBER( ValueGroup.NUMBER, (byte) 8 ),
        // TODO SPATIAL

        ZONED_DATE_TIME_ARRAY( ValueGroup.ZONED_DATE_TIME_ARRAY, (byte) 9 ),
        LOCAL_DATE_TIME_ARRAY( ValueGroup.LOCAL_DATE_TIME_ARRAY, (byte) 10 ),
        DATE_ARRAY( ValueGroup.DATE_ARRAY, (byte) 11 ),
        ZONED_TIME_ARRAY( ValueGroup.ZONED_TIME_ARRAY, (byte) 12 ),
        LOCAL_TIME_ARRAY( ValueGroup.LOCAL_TIME_ARRAY, (byte) 13 ),
        DURATION_ARRAY( ValueGroup.DURATION_ARRAY, (byte) 14 ),
        TEXT_ARRAY( ValueGroup.TEXT_ARRAY, (byte) 15 ),
        BOOLEAN_ARRAY( ValueGroup.BOOLEAN_ARRAY, (byte) 16 ),
        NUMBER_ARRAY( ValueGroup.NUMBER_ARRAY, (byte) 17 );
        // TODO SPATIAL_ARRAY

        private final ValueGroup valueGroup;
        final byte typeId;

        Type( ValueGroup valueGroup, byte typeId )
        {
            this.valueGroup = valueGroup;
            this.typeId = typeId;
        }
    }

    static final Type[] TYPES = Type.values();
    static final Type[] TYPE_BY_ID = new Type[TYPES.length];
    static final Type LOWEST_TYPE_BY_VALUE_GROUP = Collections.min( Arrays.asList( TYPES ), TYPE_COMPARATOR );
    static final Type HIGHEST_TYPE_BY_VALUE_GROUP = Collections.max( Arrays.asList( TYPES ), TYPE_COMPARATOR );
    static final Type[] TYPE_BY_GROUP = new Type[ValueGroup.values().length];
    static
    {
        for ( Type type : TYPES )
        {
            TYPE_BY_ID[type.typeId] = type;
        }
        for ( Type type : TYPES )
        {
            TYPE_BY_GROUP[type.valueGroup.ordinal()] = type;
        }
    }

    GenericLayout( int numberOfSlots )
    {
        super( "NSIL", 0, 1 );
        this.numberOfSlots = numberOfSlots;
    }

    @Override
    public CompositeGenericKey newKey()
    {
        return new CompositeGenericKey( numberOfSlots );
    }

    @Override
    public CompositeGenericKey copyKey( CompositeGenericKey key, CompositeGenericKey into )
    {
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        into.copyValuesFrom( key );
        return into;
    }

    @Override
    public int keySize( CompositeGenericKey key )
    {
        return key.size();
    }

    @Override
    public void writeKey( PageCursor cursor, CompositeGenericKey key )
    {
        key.write( cursor );
    }

    @Override
    public void readKey( PageCursor cursor, CompositeGenericKey into, int keySize )
    {
        into.read( cursor, keySize );
    }

    @Override
    public boolean fixedSize()
    {
        return false;
    }

    @Override
    public void minimalSplitter( CompositeGenericKey left, CompositeGenericKey right, CompositeGenericKey into )
    {
        CompositeGenericKey.minimalSplitter( left, right, into );
    }
}
