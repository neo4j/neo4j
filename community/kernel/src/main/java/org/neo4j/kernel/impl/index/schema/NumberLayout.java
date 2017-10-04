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
abstract class NumberLayout extends Layout.Adapter<SchemaNumberKey,SchemaNumberValue>
{
    @Override
    public SchemaNumberKey newKey()
    {
        return new SchemaNumberKey();
    }

    @Override
    public SchemaNumberKey copyKey( SchemaNumberKey key,
            SchemaNumberKey into )
    {
        into.type = key.type;
        into.rawValueBits = key.rawValueBits;
        into.entityId = key.entityId;
        into.entityIdIsSpecialTieBreaker = key.entityIdIsSpecialTieBreaker;
        return into;
    }

    @Override
    public SchemaNumberValue newValue()
    {
        return SchemaNumberValue.INSTANCE;
    }

    @Override
    public int keySize()
    {
        return SchemaNumberKey.SIZE;
    }

    @Override
    public int valueSize()
    {
        return SchemaNumberValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, SchemaNumberKey key )
    {
        cursor.putByte( key.type );
        cursor.putLong( key.rawValueBits );
        cursor.putLong( key.entityId );
    }

    @Override
    public void writeValue( PageCursor cursor, SchemaNumberValue value )
    {
    }

    @Override
    public void readKey( PageCursor cursor, SchemaNumberKey into )
    {
        into.type = cursor.getByte();
        into.rawValueBits = cursor.getLong();
        into.entityId = cursor.getLong();
    }

    @Override
    public void readValue( PageCursor cursor, SchemaNumberValue into )
    {
    }
}
