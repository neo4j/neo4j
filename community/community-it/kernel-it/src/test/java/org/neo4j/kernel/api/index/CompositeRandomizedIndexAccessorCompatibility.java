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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.exact;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " IndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class CompositeRandomizedIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    public CompositeRandomizedIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite, IndexDescriptor descriptor )
    {
        super( testSuite, descriptor );
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Exact extends CompositeRandomizedIndexAccessorCompatibility
    {
        public Exact( IndexProviderCompatibilityTestSuite testSuite )
        {
            // composite index of 4 properties
            super( testSuite, TestIndexDescriptorFactory.forLabel( 1000, 100, 101, 102, 103 ) );
        }

        @Test
        public void testExactMatchOnRandomCompositeValues() throws Exception
        {
            // given
            ValueType[] types = randomSetOfSupportedTypes();
            List<IndexEntryUpdate<?>> updates = new ArrayList<>();
            Set<ValueTuple> duplicateChecker = new HashSet<>();
            for ( long id = 0; id < 30_000; id++ )
            {
                IndexEntryUpdate<SchemaDescriptor> update;
                do
                {
                    update = add( id, descriptor.schema(),
                            random.randomValues().nextValueOfTypes( types ),
                            random.randomValues().nextValueOfTypes( types ),
                            random.randomValues().nextValueOfTypes( types ),
                            random.randomValues().nextValueOfTypes( types ) );
                }
                while ( !duplicateChecker.add( ValueTuple.of( update.values() ) ) );
                updates.add( update );
            }
            updateAndCommit( updates );

            // when
            for ( IndexEntryUpdate<?> update : updates )
            {
                // then
                List<Long> hits = query(
                        exact( 100, update.values()[0] ),
                        exact( 101, update.values()[1] ),
                        exact( 102, update.values()[2] ),
                        exact( 103, update.values()[3] ) );
                assertEquals( update + " " + hits.toString(), 1, hits.size() );
                assertThat( single( hits ), equalTo( update.getEntityId() ) );
            }
        }
    }

    @Ignore( "Not a test. This is a compatibility suite" )
    public static class Range extends CompositeRandomizedIndexAccessorCompatibility
    {
        public Range( IndexProviderCompatibilityTestSuite testSuite )
        {
            // composite index of 2 properties
            super( testSuite, TestIndexDescriptorFactory.forLabel( 1000, 100, 101 ) );
        }

        /**
         * All entries in composite index look like (booleanValue, randomValue ).
         * Range queries in composite only work if all predicates before it is exact.
         * We use boolean values for exact part so that we get some real ranges to work
         * on in second composite slot where the random values are.
         */
        @Test
        public void testRangeMatchOnRandomValues() throws Exception
        {
            Assume.assumeTrue( "Assume support for granular composite queries", testSuite.supportsGranularCompositeQueries() );
            // given
            ValueType[] types = randomSetOfSupportedAndSortableTypes();
            List<ValueTuple> values = generateValuesFromType( types );
            List<IndexEntryUpdate<?>> updates = generateUpdatesFromValues( values );
            updateAndCommit( updates );
            TreeSet<IndexEntryUpdate> sortedValues = new TreeSet<>( ( u1, u2 ) -> ValueTuple.COMPARATOR.compare(
                    ValueTuple.of( u1.values()[0], u1.values()[1] ),
                    ValueTuple.of( u2.values()[0], u2.values()[1] ) ) );
            sortedValues.addAll( updates );

            for ( int i = 0; i < 100; i++ )
            {
                Value booleanValue = random.randomValues().nextBooleanValue();
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

                // when
                List<Long> expectedIds = expectedIds( sortedValues, booleanValue, from, to, fromInclusive, toInclusive );

                // Depending on order capabilities we verify ids or order and ids.
                IndexQuery[] predicates = new IndexQuery[]{
                        IndexQuery.exact( 100, booleanValue ),
                        IndexQuery.range( 101, from, fromInclusive, to, toInclusive )};
                ValueCategory[] valueCategories = getValueCategories( predicates );
                IndexOrder[] indexOrders = indexProvider.getCapability( descriptor ).orderCapability( valueCategories );
                for ( IndexOrder order : indexOrders )
                {
                    List<Long> actualIds = assertInOrder( order, predicates );
                    actualIds.sort( Long::compare );
                    // then
                    assertThat( actualIds, equalTo( expectedIds ) );
                }
            }
        }

        public ValueCategory[] getValueCategories( IndexQuery[] predicates )
        {
            return Arrays.stream( predicates )
                                .map( iq -> iq.valueGroup().category() )
                                .toArray( ValueCategory[]::new );
        }

        public List<Long> expectedIds( TreeSet<IndexEntryUpdate> sortedValues, Value booleanValue, Value from, Value to, boolean fromInclusive,
                boolean toInclusive )
        {
            return sortedValues.subSet(
                                add( 0, descriptor.schema(), booleanValue, from ), fromInclusive,
                                add( 0, descriptor.schema(), booleanValue, to ), toInclusive )
                                .stream()
                                .map( IndexEntryUpdate::getEntityId )
                                .sorted( Long::compare )
                                .collect( Collectors.toList() );
        }

        private List<ValueTuple> generateValuesFromType( ValueType[] types )
        {
            List<ValueTuple> values = new ArrayList<>();
            Set<ValueTuple> duplicateChecker = new HashSet<>();
            for ( long i = 0; i < 30_000; i++ )
            {
                ValueTuple value;
                do
                {
                    value = ValueTuple.of(
                            // Use boolean for first slot in composite because we will use exact match on this part.x
                            random.randomValues().nextBooleanValue(),
                            random.randomValues().nextValueOfTypes( types ) );
                }
                while ( !duplicateChecker.add( value ) );
                values.add( value );
            }
            return values;
        }

        private List<IndexEntryUpdate<?>> generateUpdatesFromValues( List<ValueTuple> values )
        {
            List<IndexEntryUpdate<?>> updates = new ArrayList<>();
            int id = 0;
            for ( ValueTuple value : values )
            {
                updates.add( add( id++, descriptor.schema(), (Object[]) value.getValues() ) );
            }
            return updates;
        }
    }
}
