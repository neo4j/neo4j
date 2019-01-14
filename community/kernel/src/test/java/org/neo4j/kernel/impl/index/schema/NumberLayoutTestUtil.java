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

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class NumberLayoutTestUtil extends LayoutTestUtil<NumberSchemaKey,NativeSchemaValue>
{
    private static final Number[] ALL_EXTREME_VALUES = new Number[]
            {
                    Byte.MAX_VALUE,
                    Byte.MIN_VALUE,
                    Short.MAX_VALUE,
                    Short.MIN_VALUE,
                    Integer.MAX_VALUE,
                    Integer.MIN_VALUE,
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Float.MAX_VALUE,
                    -Float.MAX_VALUE,
                    Double.MAX_VALUE,
                    -Double.MAX_VALUE,
                    Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY,
                    0,
                    // These two values below coerce to the same double
                    1234567890123456788L,
                    1234567890123456789L
            };

    NumberLayoutTestUtil( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        super( schemaIndexDescriptor );
    }

    @Override
    IndexQuery rangeQuery( Value from, boolean fromInclusive, Value to, boolean toInclusive )
    {
        return IndexQuery.range( 0, from, fromInclusive, to, toInclusive );
    }

    @Override
    int compareIndexedPropertyValue( NumberSchemaKey key1, NumberSchemaKey key2 )
    {
        int typeCompare = Byte.compare( key1.type, key2.type );
        if ( typeCompare == 0 )
        {
            return Long.compare( key1.rawValueBits, key2.rawValueBits );
        }
        return typeCompare;
    }

    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( ALL_EXTREME_VALUES );
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( ALL_EXTREME_VALUES, ALL_EXTREME_VALUES ) );
    }

    @Override
    Value newUniqueValue( RandomRule random, Set<Object> uniqueCompareValues, List<Value> uniqueValues )
    {
        Number value;
        Double compareValue;
        do
        {
            value = random.numberPropertyValue();
            compareValue = value.doubleValue();
        }
        while ( !uniqueCompareValues.add( compareValue ) );
        Value storableValue = Values.of( value );
        uniqueValues.add( storableValue );
        return storableValue;
    }
}
