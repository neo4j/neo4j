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

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.StringSchemaKey.ENTITY_ID_SIZE;

/**
 * {@link Layout} for strings.
 */
abstract class StringLayout extends Layout.Adapter<StringSchemaKey,NativeSchemaValue>
{
    @Override
    public StringSchemaKey newKey()
    {
        return new StringSchemaKey();
    }

    @Override
    public StringSchemaKey copyKey( StringSchemaKey key, StringSchemaKey into )
    {
        // TODO when we have reuse of byte[] take that into consideration here too
        into.bytes = key.bytes.clone();
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
    public int keySize( StringSchemaKey key )
    {
        return key.size();
    }

    @Override
    public int valueSize( NativeSchemaValue value )
    {
        return NativeSchemaValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, StringSchemaKey key )
    {
        cursor.putLong( key.getEntityId() );
        cursor.putBytes( key.bytes );
    }

    @Override
    public void writeValue( PageCursor cursor, NativeSchemaValue value )
    {
    }

    @Override
    public void readKey( PageCursor cursor, StringSchemaKey into, int keySize )
    {
        // TODO consider reusing byte[] instances somehow
        if ( keySize < ENTITY_ID_SIZE )
        {
            into.bytes = null;
            return;
        }
        into.setEntityId( cursor.getLong() );
        into.bytes = new byte[keySize - ENTITY_ID_SIZE];
        cursor.getBytes( into.bytes );
    }

    @Override
    public void readValue( PageCursor cursor, NativeSchemaValue into, int valueSize )
    {
    }

    @Override
    public boolean fixedSize()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return format( "%s[version:%d.%d, identifier:%d]", getClass().getSimpleName(), majorVersion(), minorVersion(), identifier() );
    }
}
