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
import java.util.Set;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class DurationLayoutTestUtil extends LayoutTestUtil<DurationSchemaKey, NativeSchemaValue>
{
    private static final DurationValue[] ALL_EXTREME_VALUES = new DurationValue[]
    {
            DurationValue.duration( -999999999L * 12 * 2, 0, 0, 0),
            DurationValue.duration( 999999999L * 12 * 2, 0, 0, 0),
            DurationValue.duration( 0, -999999999L * 12 * 28, 0, 0),
            DurationValue.duration( 0, 999999999L * 12 * 28, 0, 0),
            DurationValue.duration( 0, 0, Long.MIN_VALUE, 0),
            DurationValue.duration( 0, 0, Long.MAX_VALUE, 0),
            DurationValue.duration( 0, 0, 0, Long.MIN_VALUE),
            DurationValue.duration( 0, 0, 0, Long.MAX_VALUE),
    };

    public static DurationValue randomDuration( Randoms random )
    {
        // not using random.randomDuration, since it cannot mix durations greater and smaller than 1 day
        return DurationValue.duration( 0, 0, random.nextLong(), random.nextLong( -999_999_999, 999_999_999 ) );
    }

    DurationLayoutTestUtil( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        super( schemaIndexDescriptor );
    }

    @Override
    Layout<DurationSchemaKey,NativeSchemaValue> createLayout()
    {
        return new DurationLayout();
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdates()
    {
        return someUpdatesWithDuplicateValues();
    }

    @Override
    IndexQuery rangeQuery( Value from, boolean fromInclusive, Value to, boolean toInclusive )
    {
        return IndexQuery.range( 0, from, fromInclusive, to, toInclusive );
    }

    @Override
    int compareIndexedPropertyValue( DurationSchemaKey key1, DurationSchemaKey key2 )
    {
        return Values.COMPARATOR.compare( key1.asValue(), key2.asValue() );
    }

    @Override
    Value newUniqueValue( RandomRule random, Set<Object> uniqueCompareValues, List<Value> uniqueValues )
    {
        DurationValue candidate;
        do
        {
            candidate = randomDuration( random.randoms() );
        }
        while ( !uniqueCompareValues.add( candidate ) );
        uniqueValues.add( candidate );
        return candidate;
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
