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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.gis.spatial.index.Envelope;
import org.neo4j.gis.spatial.index.curves.HilbertSpaceFillingCurve2D;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SpatialLayoutTestUtil extends LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue>
{
    private CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private SpaceFillingCurve curve = new HilbertSpaceFillingCurve2D( new Envelope( -180, 180, -90, 90 ), 5 );

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
        // multiply value with 5 to make sure the points aren't too close together, avoiding false positives that will be filtered in higher level later
        return Values.pointValue( CoordinateReferenceSystem.WGS84, 3 * value.doubleValue(), 3 * value.doubleValue() );
    }

    @Override
    int compareIndexedPropertyValue( SpatialSchemaKey key1, SpatialSchemaKey key2 )
    {
        return Long.compare( key1.rawValueBits, key2.rawValueBits );
    }

    @Override
    Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator( RandomRule random )
    {
        double fractionDuplicates = fractionDuplicates();
        return new PrefetchingIterator<IndexEntryUpdate<IndexDescriptor>>()
        {
            private final Set<Long> uniqueCompareValues = new HashSet<>();
            private final List<Value> uniqueValues = new ArrayList<>();
            private long currentEntityId;

            @Override
            protected IndexEntryUpdate<IndexDescriptor> fetchNextOrNull()
            {
                Value value;
                if ( fractionDuplicates > 0 && !uniqueValues.isEmpty() && random.nextFloat() < fractionDuplicates )
                {
                    value = existingNonUniqueValue( random );
                }
                else
                {
                    value = newUniqueValue( random );
                }

                return add( currentEntityId++, value );
            }

            private Value newUniqueValue( RandomRule randomRule )
            {
                double x, y;
                PointValue pointValue;
                Long compareValue;
                do
                {
                    x = randomRule.nextDouble() * 360 - 180;
                    y = randomRule.nextDouble() * 180 - 90;
                    pointValue = Values.pointValue( CoordinateReferenceSystem.WGS84, x, y );
                    compareValue = curve.derivedValueFor( pointValue.coordinate() );
                }
                while ( !uniqueCompareValues.add( compareValue ) );
                uniqueValues.add( pointValue );
                return pointValue;
            }

            private Value existingNonUniqueValue( RandomRule randomRule )
            {
                return uniqueValues.get( randomRule.nextInt( uniqueValues.size() ) );
            }
        };
    }
}
