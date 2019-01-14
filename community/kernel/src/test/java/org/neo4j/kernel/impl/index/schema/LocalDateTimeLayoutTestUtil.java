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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class LocalDateTimeLayoutTestUtil extends LayoutTestUtil<LocalDateTimeSchemaKey, NativeSchemaValue>
{
    private static final LocalDateTime[] ALL_EXTREME_VALUES = new LocalDateTime[]
    {
            LocalDateTime.of( -999999999, 1, 1, 0, 0, 0,  0),
            LocalDateTime.of( 999999999, 12, 31, 23, 59, 59,  999_999_999),
            LocalDateTime.of( 0, 1, 1, 0,0,0,0 ),
            LocalDateTime.of( 0, 1, 1, 0,0,0,1),
            LocalDateTime.of( 0, 1, 1, 0,0,0,2),
            LocalDateTime.of( -1, 12, 31, 23,59,59,999_999_999 )
    };

    public static LocalDateTimeValue randomLocalDateTime( Randoms random )
    {
        return LocalDateTimeValue.localDateTime( random.randomLocalDateTime() );
    }

    LocalDateTimeLayoutTestUtil( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        super( schemaIndexDescriptor );
    }

    @Override
    Layout<LocalDateTimeSchemaKey,NativeSchemaValue> createLayout()
    {
        return new LocalDateTimeLayout();
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
    int compareIndexedPropertyValue( LocalDateTimeSchemaKey key1, LocalDateTimeSchemaKey key2 )
    {
        return Values.COMPARATOR.compare( key1.asValue(), key2.asValue() );
    }

    @Override
    Value newUniqueValue( RandomRule random, Set<Object> uniqueCompareValues, List<Value> uniqueValues )
    {
        LocalDateTimeValue candidate;
        do
        {
            candidate = randomLocalDateTime( random.randoms() );
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
