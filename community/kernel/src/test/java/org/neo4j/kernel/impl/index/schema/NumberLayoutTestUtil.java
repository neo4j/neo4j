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

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

abstract class NumberLayoutTestUtil extends LayoutTestUtil<NumberSchemaKey,NativeSchemaValue>
{
    NumberLayoutTestUtil( IndexDescriptor indexDescriptor )
    {
        super( indexDescriptor );
    }

    @Override
    IndexQuery rangeQuery( Number from, boolean fromInclusive, Number to, boolean toInclusive )
    {
        return IndexQuery.range( 0, from, fromInclusive, to, toInclusive );
    }

    @Override
    Value asValue( Number value )
    {
        return Values.of( value );
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

    @Override
    Iterator<IndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator( RandomRule random )
    {
        double fractionDuplicates = fractionDuplicates();
        return new PrefetchingIterator<IndexEntryUpdate<IndexDescriptor>>()
        {
            private final Set<Double> uniqueCompareValues = new HashSet<>();
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
                Number value;
                Double compareValue;
                do
                {
                    value = randomRule.numberPropertyValue();
                    compareValue = value.doubleValue();
                }
                while ( !uniqueCompareValues.add( compareValue ) );
                Value storableValue = asValue( value );
                uniqueValues.add( storableValue );
                return storableValue;
            }

            private Value existingNonUniqueValue( RandomRule randomRule )
            {
                return uniqueValues.get( randomRule.nextInt( uniqueValues.size() ) );
            }
        };
    }
}
