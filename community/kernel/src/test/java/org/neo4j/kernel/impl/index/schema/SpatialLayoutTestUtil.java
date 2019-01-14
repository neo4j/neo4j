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

import org.apache.commons.lang3.ArrayUtils;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

public class SpatialLayoutTestUtil extends LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue>
{
    private static final PointValue[] ALL_EXTREME_VALUES = new PointValue[]
    {
            Values.pointValue( WGS84, -180, -90 ),
            Values.pointValue( WGS84, -180, 90 ),
            Values.pointValue( WGS84, -1, -1 ),
            Values.pointValue( WGS84, 0, 0 ),
            Values.pointValue( WGS84, 180, -90 ),
            Values.pointValue( WGS84, 180, 90 ),
    };

    public static PointValue randomPoint( Randoms random )
    {
        Random randomm = random.random();
        double x = randomm.nextDouble() * 360 - 180;
        double y = randomm.nextDouble() * 180 - 90;
        return Values.pointValue( WGS84, x, y );
    }

    private final CoordinateReferenceSystem crs;
    private final SpaceFillingCurve curve;

    SpatialLayoutTestUtil( SchemaIndexDescriptor descriptor, SpaceFillingCurveSettings settings, CoordinateReferenceSystem crs )
    {
        super( descriptor );
        this.curve = settings.curve();
        this.crs = crs;
        // The layout is the same, but we might consider supporting other CRS here, too.
        assert crs == CoordinateReferenceSystem.WGS84;
    }

    @Override
    Layout<SpatialSchemaKey,NativeSchemaValue> createLayout()
    {
        return new SpatialLayout( crs, curve );
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdates()
    {
        return someUpdatesWithDuplicateValues();
    }

    @Override
    IndexQuery rangeQuery( Value from, boolean fromInclusive, Value to, boolean toInclusive )
    {
        return IndexQuery.range( 0, from , fromInclusive, to, toInclusive );
    }

    @Override
    int compareIndexedPropertyValue( SpatialSchemaKey key1, SpatialSchemaKey key2 )
    {
        return Long.compare( key1.rawValueBits, key2.rawValueBits );
    }

    @Override
    Value newUniqueValue( RandomRule random, Set<Object> uniqueCompareValues, List<Value> uniqueValues )
    {
        PointValue pointValue;
        Long compareValue;
        do
        {
            pointValue = randomPoint( random.randoms() );
            compareValue = curve.derivedValueFor( pointValue.coordinate() );
        }
        while ( !uniqueCompareValues.add( compareValue ) );
        uniqueValues.add( pointValue );
        return pointValue;
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( ALL_EXTREME_VALUES );
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( ALL_EXTREME_VALUES, ALL_EXTREME_VALUES ) );
    }
}
