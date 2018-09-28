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

import java.time.OffsetTime;
import java.time.ZoneOffset;

import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public class TimeValueCreatorUtil extends ValueCreatorUtil<ZonedTimeIndexKey,NativeIndexValue>
{
    static long MAX_NANOS_PER_DAY = 86399999999999L;

    private static final OffsetTime[] ALL_EXTREME_VALUES = new OffsetTime[]
    {
            OffsetTime.of( 0,0,0,0, ZoneOffset.ofHours( -18 ) ),
            OffsetTime.of( 0,0,0,0, ZoneOffset.ofHours( 18 ) ),
            OffsetTime.of( 12,0,0,0, ZoneOffset.ofHours( -18 ) ),
            OffsetTime.of( 12,0,0,0, ZoneOffset.ofHours( 18 ) ),
            OffsetTime.of( 23,59,59,999_999_999, ZoneOffset.ofHours( 18 ) ),
            OffsetTime.of( 23,59,59,999_999_999, ZoneOffset.ofHours( -18 ) ),
    };

    TimeValueCreatorUtil( StoreIndexDescriptor indexDescriptor )
    {
        super( indexDescriptor );
    }

    @Override
    RandomValues.Type[] supportedTypes()
    {
        return RandomValues.typesOfGroup( ValueGroup.ZONED_TIME );
    }

    @Override
    int compareIndexedPropertyValue( ZonedTimeIndexKey key1, ZonedTimeIndexKey key2 )
    {
        return Values.COMPARATOR.compare( key1.asValue(), key2.asValue() );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( ALL_EXTREME_VALUES );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( ALL_EXTREME_VALUES, ALL_EXTREME_VALUES ) );
    }
}
