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
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.NativeIndexValue.INSTANCE;
import static org.neo4j.values.storable.Values.stringValue;

@ExtendWith( RandomExtension.class )
class NativeDistinctValuesProgressorTest
{
    private static final Config config = Config.defaults();
    private static final IndexSpecificSpaceFillingCurveSettings specificSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( config );
    private final GenericLayout layout = new GenericLayout( 1, specificSettings );

    @Inject
    private RandomRule random;

    @Test
    void shouldCountDistinctValues()
    {
        // given
        Value[] strings = generateRandomStrings();
        DataCursor source = new DataCursor( asHitData( strings ) );
        GatheringNodeValueClient client = new GatheringNodeValueClient();

        // when
        NativeDistinctValuesProgressor<GenericKey,NativeIndexValue> progressor =
                new NativeDistinctValuesProgressor<>( source, client, layout, layout::compareValue );
        client.initialize( null, progressor, new IndexQuery[0], IndexOrder.NONE, true, false );
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

    private static Map<Value, MutableInt> asDistinctCounts( Value[] strings )
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
            strings[i] = stringValue( String.valueOf( random.nextInt( 1_000 ) ) );
        }
        Arrays.sort( strings, Values.COMPARATOR );
        return strings;
    }

    private Collection<Pair<GenericKey,NativeIndexValue>> asHitData( Value[] strings )
    {
        Collection<Pair<GenericKey,NativeIndexValue>> data = new ArrayList<>( strings.length );
        for ( int i = 0; i < strings.length; i++ )
        {
            GenericKey key = layout.newKey();
            key.initialize( i );
            key.initFromValue( 0, strings[i], NEUTRAL );
            data.add( Pair.of( key, INSTANCE ) );
        }
        return data;
    }

    private static class DataCursor implements Seeker<GenericKey,NativeIndexValue>
    {
        private final Iterator<Pair<GenericKey,NativeIndexValue>> iterator;
        private Pair<GenericKey,NativeIndexValue> current;

        DataCursor( Collection<Pair<GenericKey,NativeIndexValue>> data )
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
        public GenericKey key()
        {
            return current.getKey();
        }

        @Override
        public NativeIndexValue value()
        {
            return current.getValue();
        }
    }
}
