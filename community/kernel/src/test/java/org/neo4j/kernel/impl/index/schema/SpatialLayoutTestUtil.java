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

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SpatialLayoutTestUtil extends LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue>
{
    private CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private SpaceFillingCurve curve = new HilbertSpaceFillingCurve2D( new Envelope( -180, 180, -90, 90 ) );

    SpatialLayoutTestUtil()
    {
        super( IndexDescriptorFactory.forLabel( 42, 666 ) );
    }

    @Override
    Layout<SpatialSchemaKey,NativeSchemaValue> createLayout()
    {
        return new SpatialLayoutNonUnique( crs, curve );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdates()
    {
        return someSpatialUpdatesWithDuplicateValues();
    }

    @Override
    protected double fractionDuplicates()
    {
        return 0.1;
    }

    @Override
    IndexQuery rangeQuery( Number from, boolean fromInclusive, Number to, boolean toInclusive )
    {
        return IndexQuery.range( 0, (PointValue) asValue( from ), fromInclusive, (PointValue) asValue( to ), toInclusive );
    }

    @Override
    Value asValue( Number value )
    {
        return Values.pointValue( CoordinateReferenceSystem.WGS84, value.doubleValue(), value.doubleValue() );
    }

    // TODO fix this comparison
    @Override
    int compareIndexedPropertyValue( SpatialSchemaKey key1, SpatialSchemaKey key2 )
    {
        int typeCompare = Byte.compare( key1.type, key2.type );
        if ( typeCompare == 0 )
        {
            return Long.compare( key1.rawValueBits, key2.rawValueBits );
        }
        return typeCompare;
    }
}
