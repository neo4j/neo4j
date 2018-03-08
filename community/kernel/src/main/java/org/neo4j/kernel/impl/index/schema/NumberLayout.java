/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for numbers where numbers doesn't need to be unique.
 */
abstract class NumberLayout extends Layout.Adapter<NumberSchemaKey,NativeSchemaValue>
{
    @Override
    public NumberSchemaKey newKey()
    {
        return new NumberSchemaKey();
    }

    @Override
    public NumberSchemaKey copyKey( NumberSchemaKey key, NumberSchemaKey into )
    {
        into.type = key.type;
        into.rawValueBits = key.rawValueBits;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        return into;
    }

    @Override
    public NativeSchemaValue newValue()
    {
        return NativeSchemaValue.INSTANCE;
    }

    @Override
    public int keySize( NumberSchemaKey key )
    {
        return NumberSchemaKey.SIZE;
    }

    @Override
    public int valueSize( NativeSchemaValue value )
    {
        return NativeSchemaValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, NumberSchemaKey key )
    {
        cursor.putByte( key.type );
        cursor.putLong( key.rawValueBits );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void writeValue( PageCursor cursor, NativeSchemaValue value )
    {
    }

    @Override
    public void readKey( PageCursor cursor, NumberSchemaKey into, int keySize )
    {
        into.type = cursor.getByte();
        into.rawValueBits = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, NativeSchemaValue into, int valueSize )
    {
    }

    @Override
    public int compare( NumberSchemaKey o1, NumberSchemaKey o2 )
    {
        int comparison = o1.compareValueTo( o2 );
        if ( comparison == 0 )
        {
            // This is a special case where we need also compare entityId to support inclusive/exclusive
            if ( o1.getCompareId() & o2.getCompareId() )
            {
                return Long.compare( o1.getEntityId(), o2.getEntityId() );
            }
        }
        return comparison;
    }

    @Override
    public boolean fixedSize()
    {
        return true;
    }
}
