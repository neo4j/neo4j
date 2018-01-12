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
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * {@link Layout} for numbers where numbers doesn't need to be unique.
 */
public abstract class SpatialLayout extends Layout.Adapter<SpatialSchemaKey,NativeSchemaValue>
{
    private CoordinateReferenceSystem crs;

    public SpatialLayout( CoordinateReferenceSystem crs )
    {
        this.crs = crs;
    }

    @Override
    public SpatialSchemaKey newKey()
    {
        return new SpatialSchemaKey(crs);
    }

    @Override
    public SpatialSchemaKey copyKey( SpatialSchemaKey key,
            SpatialSchemaKey into )
    {
        into.rawValueBits = key.rawValueBits;
        into.setEntityId( key.getEntityId() );
        into.setEntityIdIsSpecialTieBreaker( key.getEntityIdIsSpecialTieBreaker() );
        into.crs = key.crs;
        return into;
    }

    @Override
    public NativeSchemaValue newValue()
    {
        return NativeSchemaValue.INSTANCE;
    }

    @Override
    public int keySize()
    {
        return SpatialSchemaKey.SIZE;
    }

    @Override
    public int valueSize()
    {
        return NativeSchemaValue.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, SpatialSchemaKey key )
    {
        cursor.putLong( key.rawValueBits );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void writeValue( PageCursor cursor, NativeSchemaValue value )
    {
    }

    @Override
    public void readKey( PageCursor cursor, SpatialSchemaKey into )
    {
        into.rawValueBits = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }

    @Override
    public void readValue( PageCursor cursor, NativeSchemaValue into )
    {
    }
}
