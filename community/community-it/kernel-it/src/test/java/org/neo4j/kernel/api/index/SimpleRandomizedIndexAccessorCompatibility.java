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
package org.neo4j.kernel.api.index;

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class SimpleRandomizedIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public SimpleRandomizedIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, TestIndexDescriptorFactory.forLabel( 1000, 100 ) );
    }

    @Test
    public void testExactMatchOnRandomValues() throws Exception
    {
        // given
        ValueType[] types = randomSetOfSupportedTypes();
        List<Value> values = generateValuesFromType( types );
        List<IndexEntryUpdate<?>> updates = generateUpdatesFromValues( values );
        updateAndCommit( updates );

        // when
        for ( IndexEntryUpdate<?> update : updates )
        {
            // then
            List<Long> hits = query( IndexQuery.exact( 0, update.values()[0] ) );
            assertEquals( hits.toString(), 1, hits.size() );
            assertThat( single( hits ), equalTo( update.getEntityId() ) );
        }
    }

    @Test
    public void testRangeMatchInOrderOnRandomValues() throws Exception
    {
        Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );
        // given
        ValueType[] types = randomSetOfSupportedAndSortableTypes();
        List<Value> values = generateValuesFromType( types );
        List<IndexEntryUpdate<?>> updates = generateUpdatesFromValues( values );
        updateAndCommit( updates );
        TreeSet<IndexEntryUpdate> sortedValues = new TreeSet<>( ( u1, u2 ) -> Values.COMPARATOR.compare( u1.values()[0], u2.values()[0] ) );
        sortedValues.addAll( updates );

        for ( int i = 0; i < 100; i++ )
        {
            // Construct a random range query of random value type
            ValueType type = random.among( types );
            Value from = random.randomValues().nextValueOfType( type );
            Value to = random.randomValues().nextValueOfType( type );
            if ( Values.COMPARATOR.compare( from, to ) > 0 )
            {
                Value tmp = from;
                from = to;
                to = tmp;
            }
            boolean fromInclusive = random.nextBoolean();
            boolean toInclusive = random.nextBoolean();

            // Expected result based on query
            IndexQuery.RangePredicate<?> predicate = IndexQuery.range( 0, from, fromInclusive, to, toInclusive );
            List<Long> expectedIds = expectedIds( sortedValues, from, to, fromInclusive, toInclusive );

            // Depending on order capabilities we verify ids or order and ids.
            IndexOrder[] indexOrders = indexProvider.getCapability( descriptor ).orderCapability( predicate.valueGroup().category() );
            for ( IndexOrder order : indexOrders )
            {
                List<Long> actualIds = assertInOrder( order, predicate );
                actualIds.sort( Long::compare );
                // then
                assertThat( actualIds, equalTo( expectedIds ) );
            }
        }
    }

    private List<Long> expectedIds( TreeSet<IndexEntryUpdate> sortedValues, Value from, Value to, boolean fromInclusive, boolean toInclusive )
    {
        return sortedValues.subSet( add( 0, descriptor.schema(), from ), fromInclusive, add( 0, descriptor.schema(), to ), toInclusive )
                .stream()
                .map( IndexEntryUpdate::getEntityId )
                .sorted( Long::compare )
                .collect( Collectors.toList() );
    }

    private List<Value> generateValuesFromType( ValueType[] types )
    {
        List<Value> values = new ArrayList<>();
        Set<Value> duplicateChecker = new HashSet<>();
        for ( long i = 0; i < 30_000; i++ )
        {
            Value value;
            do
            {
                value = random.randomValues().nextValueOfTypes( types );
            }
            while ( !duplicateChecker.add( value ) );
            values.add( value );
        }
        return values;
    }

    private List<IndexEntryUpdate<?>> generateUpdatesFromValues( List<Value> values )
    {
        List<IndexEntryUpdate<?>> updates = new ArrayList<>();
        int id = 0;
        for ( Value value : values )
        {
            updates.add( add( id++, descriptor.schema(), value ) );
        }
        return updates;
    }
}
