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

import org.apache.commons.lang3.ArrayUtils;

import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

class SpatialValueCreatorUtil extends ValueCreatorUtil<SpatialIndexKey,NativeIndexValue>
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

    SpatialValueCreatorUtil( StoreIndexDescriptor descriptor )
    {
        super( descriptor );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdates( RandomRule randomRule )
    {
        return someUpdatesWithDuplicateValues( randomRule );
    }

    @Override
    RandomValues.Type[] supportedTypes()
    {
        return new RandomValues.Type[]{RandomValues.Type.GEOGRAPHIC_POINT};
    }

    @Override
    int compareIndexedPropertyValue( SpatialIndexKey key1, SpatialIndexKey key2 )
    {
        return Long.compare( key1.rawValueBits, key2.rawValueBits );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues( RandomRule randomRule )
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( ALL_EXTREME_VALUES, ALL_EXTREME_VALUES ) );
    }
}
