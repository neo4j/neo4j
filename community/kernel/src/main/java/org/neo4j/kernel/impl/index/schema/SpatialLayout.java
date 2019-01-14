/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * {@link Layout} for PointValues.
 */
class SpatialLayout extends SchemaLayout<SpatialSchemaKey>
{
    private SpaceFillingCurve curve;
    CoordinateReferenceSystem crs;

    SpatialLayout( CoordinateReferenceSystem crs, SpaceFillingCurve curve )
    {
        super( "UPI", 0, 1 );
        this.crs = crs;
        this.curve = curve;
    }

    SpaceFillingCurve getSpaceFillingCurve()
    {
        return curve;
    }

    @Override
    public SpatialSchemaKey newKey()
    {
        return new SpatialSchemaKey( crs, curve );
    }

    @Override
    public SpatialSchemaKey copyKey( SpatialSchemaKey key, SpatialSchemaKey into )
    {
        into.rawValueBits = key.rawValueBits;
        into.setEntityId( key.getEntityId() );
        into.setCompareId( key.getCompareId() );
        into.crs = key.crs;
        into.curve = key.curve;
        return into;
    }

    @Override
    public int keySize( SpatialSchemaKey key )
    {
        return SpatialSchemaKey.SIZE;
    }

    @Override
    public void writeKey( PageCursor cursor, SpatialSchemaKey key )
    {
        cursor.putLong( key.rawValueBits );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, SpatialSchemaKey into, int keySize )
    {
        into.rawValueBits = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }
}
