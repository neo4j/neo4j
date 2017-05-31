/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
 * {@link Layout} for numbers where numbers need to be unique.
 */
class UniqueNumberLayout implements Layout<UniqueNumberKey,UniqueNumberValue>
{
    private static final String IDENTIFIER_NAME = "UNI";

    @Override
    public UniqueNumberKey newKey()
    {
        return new UniqueNumberKey();
    }

    @Override
    public UniqueNumberKey copyKey( UniqueNumberKey key, UniqueNumberKey into )
    {
        into.value = key.value;
        into.isHighest = key.isHighest;
        return into;
    }

    @Override
    public UniqueNumberValue newValue()
    {
        return new UniqueNumberValue();
    }

    @Override
    public int keySize()
    {
        return UniqueNumberKey.SIZE;
    }

    @Override
    public int valueSize()
    {
        return UniqueNumberValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, UniqueNumberKey key )
    {
        cursor.putLong( Double.doubleToRawLongBits( key.value ) );
    }

    @Override
    public void readKey( PageCursor cursor, UniqueNumberKey into )
    {
        into.value = Double.longBitsToDouble( cursor.getLong() );
    }

    @Override
    public void writeValue( PageCursor cursor, UniqueNumberValue value )
    {
        cursor.putByte( value.type );
        cursor.putLong( value.rawValueBits );
        cursor.putLong( value.entityId );
    }

    @Override
    public void readValue( PageCursor cursor, UniqueNumberValue into )
    {
        into.type = cursor.getByte();
        into.rawValueBits = cursor.getLong();
        into.entityId = cursor.getLong();
    }

    @Override
    public long identifier()
    {
        return Layout.namedIdentifier( IDENTIFIER_NAME, UniqueNumberValue.SIZE );
    }

    @Override
    public int majorVersion()
    {
        return 0;
    }

    @Override
    public int minorVersion()
    {
        return 1;
    }

    @Override
    public int compare( UniqueNumberKey o1, UniqueNumberKey o2 )
    {
        int comparison = Double.compare( o1.value, o2.value );
        if ( comparison == 0 )
        {
            // This is a special case where we need to check if o2 is the unbounded max key.
            // This needs to be checked to be able to support storing Double.MAX_VALUE and retrieve it later.
            if ( o2.isHighest )
            {
                return -1;
            }
            if ( o1.isHighest )
            {
                return 1;
            }
        }
        return comparison;
    }
}
