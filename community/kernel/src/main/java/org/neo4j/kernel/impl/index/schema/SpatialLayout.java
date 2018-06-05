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

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.utils.InvalidValuesArgumentException;

/**
 * {@link Layout} for PointValues.
 */
class SpatialLayout extends SchemaLayout<SpatialSchemaKey>
{
    private final SpaceFillingCurveSettingsFactory curveFactory;

    SpatialLayout( SpaceFillingCurveSettingsFactory curveFactory )
    {
        super( "UPI", 0, 1 );
        this.curveFactory = curveFactory;
    }

    SpaceFillingCurve getSpaceFillingCurve( CoordinateReferenceSystem crs )
    {
        return curveFactory.settingsFor( crs ).curve();
    }

    @Override
    public SpatialSchemaKey newKey()
    {
        return new SpatialSchemaKey( curveFactory );
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
        cursor.putInt( key.crs.getTable().getTableId() );
        cursor.putInt( key.crs.getCode() );
        cursor.putLong( key.rawValueBits );
        cursor.putLong( key.getEntityId() );
    }

    @Override
    public void readKey( PageCursor cursor, SpatialSchemaKey into, int keySize )
    {
        int tableId = cursor.getInt();
        int code = cursor.getInt();
        try
        {
            if ( into.crs == null || into.crs.getTable().getTableId() != tableId || into.crs.getCode() != code )
            {
                into.crs = CoordinateReferenceSystem.get( tableId, code );
            }
        }
        catch ( InvalidValuesArgumentException | IllegalArgumentException | ArrayIndexOutOfBoundsException e )
        {
            // This can happen on bad read from page cursor which should result in shouldRetry
            into.crs = CoordinateReferenceSystem.WGS84;
        }
        into.rawValueBits = cursor.getLong();
        into.setEntityId( cursor.getLong() );
    }
}
