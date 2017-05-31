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
 * {@link Layout} for numbers where numbers doesn't need to be unique.
 */
public class NonUniqueNumberLayout implements Layout<NonUniqueNumberKey,NonUniqueNumberValue>
{
    private static final String IDENTIFIER_NAME = "NUNI";

    @Override
    public NonUniqueNumberKey newKey()
    {
        return new NonUniqueNumberKey();
    }

    @Override
    public NonUniqueNumberKey copyKey( NonUniqueNumberKey key,
            NonUniqueNumberKey into )
    {
        into.value = key.value;
        into.entityId = key.entityId;
        return into;
    }

    @Override
    public NonUniqueNumberValue newValue()
    {
        return new NonUniqueNumberValue();
    }

    @Override
    public int keySize()
    {
        return NonUniqueNumberKey.SIZE;
    }

    @Override
    public int valueSize()
    {
        return NonUniqueNumberValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, NonUniqueNumberKey key )
    {
        cursor.putLong( Double.doubleToRawLongBits( key.value ) );
        cursor.putLong( key.entityId );
    }

    @Override
    public void writeValue( PageCursor cursor, NonUniqueNumberValue key )
    {
        cursor.putByte( key.type );
        cursor.putLong( key.rawValueBits );
    }

    @Override
    public void readKey( PageCursor cursor, NonUniqueNumberKey into )
    {
        into.value = Double.longBitsToDouble( cursor.getLong() );
        into.entityId = cursor.getLong();
    }

    @Override
    public void readValue( PageCursor cursor, NonUniqueNumberValue into )
    {
        into.type = cursor.getByte();
        into.rawValueBits = cursor.getLong();
    }

    @Override
    public long identifier()
    {
        return Layout.namedIdentifier( IDENTIFIER_NAME, NonUniqueNumberValue.SIZE );
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
    public int compare( NonUniqueNumberKey o1, NonUniqueNumberKey o2 )
    {
        int compare = Double.compare( o1.value, o2.value );
        return compare != 0 ? compare : Long.compare( o1.entityId, o2.entityId );
    }
}
