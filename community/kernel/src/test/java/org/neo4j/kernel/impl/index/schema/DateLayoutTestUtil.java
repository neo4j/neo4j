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

import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class DateLayoutTestUtil extends LayoutTestUtil<DateSchemaKey, NativeSchemaValue>
{
    private static final LocalDate[] ALL_EXTREME_VALUES = new LocalDate[]
    {
            LocalDate.of( -999999999, 1, 1),
            LocalDate.of( 999999999, 12, 31),
            LocalDate.of( 0, 1, 1),
            LocalDate.of( 0, 1, 2),
            LocalDate.of( 0, 1, 3),
            LocalDate.of( -1, 12, 31)
    };

    public static DateValue randomDate( Randoms random )
    {
        return DateValue.date( random.randomDate() );
    }

    DateLayoutTestUtil( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        super( schemaIndexDescriptor );
    }

    @Override
    Layout<DateSchemaKey,NativeSchemaValue> createLayout()
    {
        return new DateLayout();
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
    int compareIndexedPropertyValue( DateSchemaKey key1, DateSchemaKey key2 )
    {
        return Values.COMPARATOR.compare( key1.asValue(), key2.asValue() );
    }

    @Override
    Value newUniqueValue( RandomRule random, Set<Object> uniqueCompareValues, List<Value> uniqueValues )
    {
        DateValue candidate;
        do
        {
            candidate = randomDate( random.randoms() );
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
