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

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaValue.INSTANCE;
import static org.neo4j.values.storable.Values.stringValue;

public class NativeDistinctValuesProgressorTest
{
    private final StringLayout layout = new StringLayout();

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldCountDistinctValues()
    {
        // given
        Value[] strings = generateRandomStrings();
        DataCursor source = new DataCursor( asHitData( strings ) );
        GatheringNodeValueClient client = new GatheringNodeValueClient();

        // when
        NativeDistinctValuesProgressor<StringSchemaKey,NativeSchemaValue> progressor =
                new NativeDistinctValuesProgressor<>( source, client, new ArrayList<>(), layout, layout::compareValue );
        Map<Value,MutableInt> expectedCounts = asDistinctCounts( strings );

        // then
        int uniqueValues = 0;
        int nonUniqueValues = 0;
        while ( progressor.next() )
        {
            Value string = client.values[0];
            MutableInt expectedCount = expectedCounts.remove( string );
            assertNotNull( expectedCount );
            assertEquals( expectedCount.intValue(), client.reference );

            if ( expectedCount.intValue() > 1 )
            {
                nonUniqueValues++;
            }
            else
            {
                uniqueValues++;
            }
        }
        assertTrue( expectedCounts.isEmpty() );
        assertTrue( uniqueValues > 0 );
        assertTrue( nonUniqueValues > 0 );
    }

    private Map<Value,MutableInt> asDistinctCounts( Value[] strings )
    {
        Map<Value,MutableInt> map = new HashMap<>();
        for ( Value string : strings )
        {
            map.computeIfAbsent( string, s -> new MutableInt( 0 ) ).increment();
        }
        return map;
    }

    private Value[] generateRandomStrings()
    {
        Value[] strings = new Value[1_000];
        for ( int i = 0; i < strings.length; i++ )
        {
            // Potential for a lot of duplicates
            strings[i] = stringValue( random.randoms().string( 1, 3, Randoms.CS_DIGITS ) );
        }
        Arrays.sort( strings, Values.COMPARATOR );
        return strings;
    }

    private Collection<Hit<StringSchemaKey,NativeSchemaValue>> asHitData( Value[] strings )
    {
        Collection<Hit<StringSchemaKey,NativeSchemaValue>> data = new ArrayList<>( strings.length );
        for ( int i = 0; i < strings.length; i++ )
        {
            StringSchemaKey key = layout.newKey();
            key.from( i, strings[i] );
            data.add( new SimpleHit<>( key, INSTANCE ) );
        }
        return data;
    }

    private static class DataCursor implements RawCursor<Hit<StringSchemaKey,NativeSchemaValue>,IOException>
    {
        private final Iterator<Hit<StringSchemaKey,NativeSchemaValue>> iterator;
        private Hit<StringSchemaKey,NativeSchemaValue> current;

        DataCursor( Collection<Hit<StringSchemaKey,NativeSchemaValue>> data )
        {
            this.iterator = data.iterator();
        }

        @Override
        public boolean next() throws RuntimeException
        {
            if ( !iterator.hasNext() )
            {
                return false;
            }
            current = iterator.next();
            return true;
        }

        @Override
        public void close() throws RuntimeException
        {
            // Nothing to close
        }

        @Override
        public Hit<StringSchemaKey,NativeSchemaValue> get()
        {
            return current;
        }
    }
}
