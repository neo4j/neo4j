/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedAndRemoved;
import org.neo4j.kernel.impl.newapi.TxStateIndexChanges.AddedWithValuesAndRemoved;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesForSuffixOrContains;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForScan;
import static org.neo4j.kernel.impl.newapi.TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

class TxStateIndexChangesTest
{
    private final IndexDescriptor index = TestIndexDescriptorFactory.forLabel( 1, 1 );

    @Test
    void shouldComputeIndexUpdatesForScanOnAnEmptyTxState()
    {
        final ReadableTransactionState state = Mockito.mock( ReadableTransactionState.class );

        // WHEN
        AddedAndRemoved changes = indexUpdatesForScan( state, index, IndexOrder.NONE );
        AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan( state, index, IndexOrder.NONE );

        // THEN
        assertTrue( changes.isEmpty() );
        assertTrue( changesWithValues.isEmpty() );
    }

    @Test
    void shouldComputeIndexUpdatesForScanWhenThereAreNewNodes()
    {
        // GIVEN
        final ReadableTransactionState state = new TxStateBuilder()
                .withAdded( 42L, "foo" )
                .withAdded( 43L, "bar" )
                .build();

        // WHEN
        AddedAndRemoved changes = indexUpdatesForScan( state, index, IndexOrder.NONE );
        AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan( state, index, IndexOrder.NONE );

        // THEN
        assertContains( changes.getAdded(), 42L, 43L );
        assertContains( changesWithValues.getAdded(), nodeWithPropertyValues( 42L, "foo" ), nodeWithPropertyValues( 43L, "bar" ) );
    }

    @Test
    void shouldComputeIndexUpdatesForScan()
    {
        assertScanWithOrder( IndexOrder.NONE );
        assertScanWithOrder( IndexOrder.ASCENDING );
    }

    @Test
    void shouldComputeIndexUpdatesForScanWithDescendingOrder()
    {
        assertScanWithOrder( IndexOrder.DESCENDING );
    }

    private void assertScanWithOrder( IndexOrder indexOrder )
    {
        // GIVEN
        final ReadableTransactionState state = new TxStateBuilder()
                .withAdded( 40L, "Aaron" )
                .withAdded( 41L, "Agatha" )
                .withAdded( 42L, "Andreas" )
                .withAdded( 43L, "Barbarella" )
                .withAdded( 44L, "Andrea" )
                .withAdded( 45L, "Aristotle" )
                .withAdded( 46L, "Barbara" )
                .withAdded( 47L, "Cinderella" )
                .build();

        // WHEN
        AddedAndRemoved changes = indexUpdatesForScan( state, index, indexOrder );
        AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan( state, index, indexOrder );

        NodeWithPropertyValues[] expectedNodesWithValues = {nodeWithPropertyValues( 40L, "Aaron" ),
                                                            nodeWithPropertyValues( 41L, "Agatha" ),
                                                            nodeWithPropertyValues( 44L, "Andrea" ),
                                                            nodeWithPropertyValues( 42L, "Andreas" ),
                                                            nodeWithPropertyValues( 45L, "Aristotle" ),
                                                            nodeWithPropertyValues( 46L, "Barbara" ),
                                                            nodeWithPropertyValues( 43L, "Barbarella" ),
                                                            nodeWithPropertyValues( 47L, "Cinderella" )};

        // THEN
        assertContains( indexOrder, changes, changesWithValues, expectedNodesWithValues );
    }

    @Test
    void shouldComputeIndexUpdatesForSeekWhenThereAreNewNodes()
    {
        // GIVEN
        final ReadableTransactionState state = new TxStateBuilder()
                .withAdded( 42L, "foo" )
                .withAdded( 43L, "bar" )
                .build();

        // WHEN
        AddedAndRemoved changes = indexUpdatesForSeek( state, index, ValueTuple.of( "bar" ) );

        // THEN
        assertContains( changes.getAdded(), 43L );
    }

    @TestFactory
    Collection<DynamicTest> rangeTests()
    {
        final ReadableTransactionState state = new TxStateBuilder()
                .withAdded( 42L, 510 )
                .withAdded( 43L, 520 )
                .withAdded( 44L, 550 )
                .withAdded( 45L, 500 )
                .withAdded( 46L, 530 )
                .withAdded( 47L, 560 )
                .withAdded( 48L, 540 )
                .build();

        final Collection<DynamicTest> tests = new ArrayList<>();

        tests.addAll( rangeTest( state, Values.of( 510 ), true, Values.of( 550 ), true,
                nodeWithPropertyValues( 42L, 510 ),
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 ),
                nodeWithPropertyValues( 44L, 550 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 510 ), true, Values.of( 550 ), false,
                nodeWithPropertyValues( 42L, 510 ),
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 510 ), false, Values.of( 550 ), true,
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 ),
                nodeWithPropertyValues( 44L, 550 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 510 ), false, Values.of( 550 ), false,
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 )
        ) );
        tests.addAll( rangeTest( state, null, false, Values.of( 550 ), true,
                nodeWithPropertyValues( 45L, 500 ),
                nodeWithPropertyValues( 42L, 510 ),
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 ),
                nodeWithPropertyValues( 44L, 550 )
        ) );
        tests.addAll( rangeTest( state, null, true, Values.of( 550 ), true,
                nodeWithPropertyValues( 45L, 500 ),
                nodeWithPropertyValues( 42L, 510 ),
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 ),
                nodeWithPropertyValues( 44L, 550 )
        ) );
        tests.addAll( rangeTest( state, null, false, Values.of( 550 ), false,
                nodeWithPropertyValues( 45L, 500 ),
                nodeWithPropertyValues( 42L, 510 ),
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 )
        ) );
        tests.addAll( rangeTest( state, null, true, Values.of( 550 ), false,
                nodeWithPropertyValues( 45L, 500 ),
                nodeWithPropertyValues( 42L, 510 ),
                nodeWithPropertyValues( 43L, 520 ),
                nodeWithPropertyValues( 46L, 530 ),
                nodeWithPropertyValues( 48L, 540 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 540 ), true, null, true,
                nodeWithPropertyValues( 48L, 540 ),
                nodeWithPropertyValues( 44L, 550 ),
                nodeWithPropertyValues( 47L, 560 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 540 ), true, null, false,
                nodeWithPropertyValues( 48L, 540 ),
                nodeWithPropertyValues( 44L, 550 ),
                nodeWithPropertyValues( 47L, 560 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 540 ), false, null, true,
                nodeWithPropertyValues( 44L, 550 ),
                nodeWithPropertyValues( 47L, 560 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 540 ), false, null, false,
                nodeWithPropertyValues( 44L, 550 ),
                nodeWithPropertyValues( 47L, 560 )
        ) );
        tests.addAll( rangeTest( state, Values.of( 560 ), false, Values.of( 800 ), true ) );

        return tests;
    }

    private Collection<DynamicTest> rangeTest( ReadableTransactionState state, Value lo, boolean includeLo, Value hi, boolean includeHi,
            NodeWithPropertyValues... expected )
    {
        return Arrays.asList( rangeTest( state, IndexOrder.NONE, lo, includeLo, hi, includeHi, expected ),
                              rangeTest( state, IndexOrder.ASCENDING, lo, includeLo, hi, includeHi, expected ),
                              rangeTest( state, IndexOrder.DESCENDING, lo, includeLo, hi, includeHi, expected ) );
    }

    private DynamicTest rangeTest( ReadableTransactionState state,
                                   IndexOrder indexOrder,
                                   Value lo,
                                   boolean includeLo,
                                   Value hi,
                                   boolean includeHi,
                                   NodeWithPropertyValues... expected )
    {
        return DynamicTest.dynamicTest( String.format( "range seek: lo=%s (incl: %s), hi=%s (incl: %s)", lo, includeLo, hi, includeHi ), () ->
        {
            // Internal production code relies on null for unbounded, and cannot cope with NO_VALUE in this case
            assert lo != NO_VALUE;
            assert hi != NO_VALUE;
            final AddedAndRemoved changes =
                    indexUpdatesForRangeSeek( state, index, new Value[0], IndexQuery.range( -1, lo, includeLo, hi, includeHi ), indexOrder );
            final AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForRangeSeek( state, index, new Value[0], IndexQuery.range( -1, lo, includeLo, hi, includeHi ), indexOrder );

            assertContains( indexOrder, changes, changesWithValues, expected );
        } );
    }

    @Nested
    class SuffixOrContains
    {

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereAreNoMatchingNodes()
        {
            // GIVEN
            final ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "foo" )
                    .withAdded( 43L, "bar" )
                    .build();

            // WHEN
            IndexQuery.StringContainsPredicate indexQuery = IndexQuery.stringContains( index.schema().getPropertyId(), stringValue( "eulav" ) );
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains( state, index, indexQuery, IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForSuffixOrContains( state, index, indexQuery, IndexOrder.NONE );

            // THEN
            assertTrue( changes.getAdded().isEmpty() );
            assertFalse( changesWithValues.getAdded().iterator().hasNext() );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekBySuffixWhenThereArePartiallyMatchingNewNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 40L, "Aaron" )
                    .withAdded( 41L, "Agatha" )
                    .withAdded( 42L, "Andreas" )
                    .withAdded( 43L, "Andrea" )
                    .withAdded( 44L, "Aristotle" )
                    .withAdded( 45L, "Barbara" )
                    .withAdded( 46L, "Barbarella" )
                    .withAdded( 47L, "Cinderella" )
                    .build();

            // WHEN
            IndexQuery.StringSuffixPredicate indexQuery = IndexQuery.stringSuffix( index.schema().getPropertyId(), stringValue( "ella" ) );
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains( state, index, indexQuery, IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForSuffixOrContains( state, index, indexQuery, IndexOrder.NONE );

            // THEN
            assertContains( changes.getAdded(), 46L, 47L );
            assertContains( changesWithValues.getAdded(),
                            nodeWithPropertyValues( 46L, "Barbarella" ),
                            nodeWithPropertyValues( 47L, "Cinderella" ) );
        }

        @Test
        void shouldComputeIndexUpdatesForSuffixWithAscendingOrder()
        {
            assertRangeSeekBySuffixForOrder( IndexOrder.ASCENDING );
        }

        @Test
        void shouldComputeIndexUpdatesForSuffixWithDescendingOrder()
        {
            assertRangeSeekBySuffixForOrder( IndexOrder.DESCENDING );
        }

        private void assertRangeSeekBySuffixForOrder( IndexOrder indexOrder )
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 40L, "Aaron" )
                    .withAdded( 41L, "Bonbon" )
                    .withAdded( 42L, "Crayfish" )
                    .withAdded( 43L, "Mayonnaise" )
                    .withAdded( 44L, "Seashell" )
                    .withAdded( 45L, "Ton" )
                    .withAdded( 46L, "Macron" )
                    .withAdded( 47L, "Tony" )
                    .withAdded( 48L, "Evon" )
                    .withAdded( 49L, "Andromeda" )
                    .build();

            // WHEN
            IndexQuery indexQuery = IndexQuery.stringSuffix( index.schema().getPropertyId(), stringValue( "on" ));
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains( state, index, indexQuery, indexOrder );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForSuffixOrContains( state, index, indexQuery, indexOrder );

            NodeWithPropertyValues[] expected = {nodeWithPropertyValues( 40L, "Aaron" ),
                                                 nodeWithPropertyValues( 41L, "Bonbon" ),
                                                 nodeWithPropertyValues( 48L, "Evon" ),
                                                 nodeWithPropertyValues( 46L, "Macron" ),
                                                 nodeWithPropertyValues( 45L, "Ton" )};

            // THEN
            assertContains( indexOrder, changes, changesWithValues, expected );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereArePartiallyMatchingNewNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 40L, "Aaron" )
                    .withAdded( 41L, "Agatha" )
                    .withAdded( 42L, "Andreas" )
                    .withAdded( 43L, "Andrea" )
                    .withAdded( 44L, "Aristotle" )
                    .withAdded( 45L, "Barbara" )
                    .withAdded( 46L, "Barbarella" )
                    .withAdded( 47L, "Cinderella" )
                    .build();

            // WHEN
            IndexQuery.StringContainsPredicate indexQuery = IndexQuery.stringContains( index.schema().getPropertyId(), stringValue( "arbar" ) );
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains( state, index, indexQuery, IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForSuffixOrContains( state, index, indexQuery, IndexOrder.NONE );

            // THEN
            assertContains( changes.getAdded(), 45L, 46L );
            assertContains( changesWithValues.getAdded(),
                            nodeWithPropertyValues( 45L, "Barbara" ),
                            nodeWithPropertyValues( 46L, "Barbarella" ) );
        }

        @Test
        void shouldComputeIndexUpdatesForContainsWithAscendingOrder()
        {
            assertRangeSeekByContainsForOrder( IndexOrder.ASCENDING );
        }

        @Test
        void shouldComputeIndexUpdatesForContainsWithDescendingOrder()
        {
            assertRangeSeekByContainsForOrder( IndexOrder.DESCENDING );
        }

        private void assertRangeSeekByContainsForOrder( IndexOrder indexOrder )
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 40L, "Smashing" )
                    .withAdded( 41L, "Bashley" )
                    .withAdded( 42L, "Crasch" )
                    .withAdded( 43L, "Mayonnaise" )
                    .withAdded( 44L, "Seashell" )
                    .withAdded( 45L, "Ton" )
                    .withAdded( 46L, "The Flash" )
                    .withAdded( 47L, "Strayhound" )
                    .withAdded( 48L, "Trashy" )
                    .withAdded( 49L, "Andromeda" )
                    .build();

            // WHEN
            IndexQuery indexQuery = IndexQuery.stringContains( index.schema().getPropertyId(), stringValue( "ash" ) );
            AddedAndRemoved changes = indexUpdatesForSuffixOrContains( state, index, indexQuery, indexOrder );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForSuffixOrContains( state, index, indexQuery, indexOrder );

            NodeWithPropertyValues[] expected = {nodeWithPropertyValues( 41L, "Bashley" ),
                                                 nodeWithPropertyValues( 44L, "Seashell" ),
                                                 nodeWithPropertyValues( 40L, "Smashing" ),
                                                 nodeWithPropertyValues( 46L, "The Flash" ),
                                                 nodeWithPropertyValues( 48L, "Trashy" )};

            // THEN
            assertContains( indexOrder, changes, changesWithValues, expected );
        }
    }

    @Nested
    class Prefix
    {

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNoMatchingNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "value42" )
                    .withAdded( 43L, "value43" )
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForRangeSeekByPrefix( state, index, new Value[0], stringValue( "eulav" ), IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForRangeSeekByPrefix( state, index, new Value[0], stringValue( "eulav" ), IndexOrder.NONE );

            // THEN
            assertTrue( changes.getAdded().isEmpty() );
            assertFalse( changesWithValues.getAdded().iterator().hasNext() );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefix()
        {
            assertRangeSeekByPrefixForOrder( IndexOrder.NONE );
            assertRangeSeekByPrefixForOrder( IndexOrder.ASCENDING );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWithDescendingOrder()
        {
            assertRangeSeekByPrefixForOrder( IndexOrder.DESCENDING );
        }

        private void assertRangeSeekByPrefixForOrder( IndexOrder indexOrder )
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 40L, "Aaron" )
                    .withAdded( 41L, "Agatha" )
                    .withAdded( 42L, "Andreas" )
                    .withAdded( 43L, "Barbarella" )
                    .withAdded( 44L, "Andrea" )
                    .withAdded( 45L, "Aristotle" )
                    .withAdded( 46L, "Barbara" )
                    .withAdded( 47L, "Andy" )
                    .withAdded( 48L, "Cinderella" )
                    .withAdded( 49L, "Andromeda" )
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForRangeSeekByPrefix( state, index, new Value[0], stringValue( "And" ), indexOrder );
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForRangeSeekByPrefix( state, index, new Value[0], stringValue( "And" ), indexOrder );

            NodeWithPropertyValues[] expected = {nodeWithPropertyValues( 44L, "Andrea" ),
                                                 nodeWithPropertyValues( 42L, "Andreas" ),
                                                 nodeWithPropertyValues( 49L, "Andromeda" ),
                                                 nodeWithPropertyValues( 47L, "Andy" )};

            // THEN
            assertContains( indexOrder, changes, changesWithValues, expected );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNonStringNodes()
        {
            // GIVEN
            final ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "barry" )
                    .withAdded( 44L, 101L )
                    .withAdded( 43L, "bar" )
                    .build();

            // WHEN
            AddedAndRemoved changes = TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix( state, index, new Value[0], stringValue( "bar" ), IndexOrder.NONE );

            // THEN
            assertContainsInOrder( changes.getAdded(),   43L, 42L );
        }
    }

    @Nested
    class CompositeIndex
    {
        private final IndexDescriptor compositeIndex = TestIndexDescriptorFactory.forLabel( 1, 1, 2 );
        private final IndexDescriptor compositeIndex3properties = TestIndexDescriptorFactory.forLabel( 1, 1, 2, 3 );

        @Test
        void shouldSeekOnAnEmptyTxState()
        {
            // GIVEN
            final ReadableTransactionState state = Mockito.mock( ReadableTransactionState.class );

            // WHEN
            AddedAndRemoved changes = indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "43value1", "43value2" ) );

            // THEN
            assertTrue( changes.isEmpty() );
        }

        @Test
        void shouldScanWhenThereAreNewNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2" )
                    .withAdded( 43L, "43value1", "43value2" )
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForScan( state, compositeIndex, IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan( state, compositeIndex, IndexOrder.NONE );

            // THEN
            assertContains( changes.getAdded(), 42L, 43L );
            assertContains( changesWithValues.getAdded(),
                            nodeWithPropertyValues( 42L, "42value1", "42value2" ),
                            nodeWithPropertyValues( 43L, "43value1", "43value2" ) );
        }

        @Test
        void shouldSeekWhenThereAreNewStringNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2" )
                    .withAdded( 43L, "43value1", "43value2" )
                    .build();
            IndexQuery.RangePredicate<?> predicate =
                    IndexQuery.range( -1, null, false, stringValue( "44val" ), true );

            // WHEN
            AddedAndRemoved changes = indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "43value1", "43value2" ) );
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForRangeSeek( state, compositeIndex, new Value[]{stringValue( "43value1" )}, predicate, IndexOrder.NONE );

            // THEN
            assertContains( changes.getAdded(), 43L );
            assertContains( changesWithValues.getAdded(), nodeWithPropertyValues( 43L, "43value1", "43value2" ) );
        }

        @Test
        void shouldSeekWhenThereAreNewNumberNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, 42001.0, 42002.0 )
                    .withAdded( 43L, 43001.0, 43002.0 )
                    .build();
            IndexQuery.RangePredicate<?> predicate =
                    IndexQuery.range( -1, doubleValue( 43000.0 ), true, null, false );

            // WHEN
            AddedAndRemoved changes = indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( 43001.0, 43002.0 ) );
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForRangeSeek( state, compositeIndex, new Value[]{doubleValue( 43001.0 )}, predicate, IndexOrder.NONE );

            // THEN
            assertContains( changes.getAdded(), 43L );
            assertContains( changesWithValues.getAdded(), nodeWithPropertyValues( 43L, 43001.0, 43002.0 ) );
        }

        @Test
        void shouldHandleMixedAddsAndRemovesEntryForScan()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2" )
                    .withAdded( 43L, "43value1", "43value2" )
                    .withRemoved( 43L, "43value1", "43value2" )
                    .withRemoved( 44L, "44value1", "44value2" )
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForScan( state, compositeIndex, IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForScan( state, compositeIndex, IndexOrder.NONE );

            // THEN
            assertContains( changes.getAdded(), 42L );
            assertContains( changesWithValues.getAdded(), nodeWithPropertyValues( 42L, "42value1", "42value2" ) );
            assertContains( changes.getRemoved(), 44L );
            assertContains( changesWithValues.getRemoved(), 44L );
        }

        @Test
        void shouldHandleMixedAddsAndRemovesEntryForSeek()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2" )
                    .withAdded( 43L, "43value1", "43value2" )
                    .withRemoved( 43L, "43value1", "43value2" )
                    .withRemoved( 44L, "44value1", "44value2" )
                    .build();

            // WHEN
            AddedAndRemoved changes42 = indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "42value1", "42value2" ) );
            AddedAndRemoved changes43 = indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "43value1", "43value2" ) );
            AddedAndRemoved changes44 = indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "44value1", "44value2" ) );

            AddedWithValuesAndRemoved changesWithValues42 =
                    indexUpdatesWithValuesForRangeSeek( state, compositeIndex, new Value[]{stringValue( "42value1" )}, null, IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues43 =
                    indexUpdatesWithValuesForRangeSeek( state, compositeIndex, new Value[]{stringValue( "43value1" )}, null, IndexOrder.NONE );
            AddedWithValuesAndRemoved changesWithValues44 =
                    indexUpdatesWithValuesForRangeSeek( state, compositeIndex, new Value[]{stringValue( "44value1" )}, null, IndexOrder.NONE );

            // THEN
            assertContains( changes42.getAdded(), 42L );
            assertTrue( changes42.getRemoved().isEmpty() );
            assertTrue( changes43.isEmpty() );
            assertTrue( changes44.getAdded().isEmpty() );
            assertContains( changes44.getRemoved(), 44L );

            assertContains( changesWithValues42.getAdded(), nodeWithPropertyValues( 42L, "42value1", "42value2" ) );
            assertTrue( changesWithValues42.getRemoved().isEmpty() );
            assertTrue( changesWithValues43.isEmpty() );
            assertFalse( changesWithValues44.getAdded().iterator().hasNext() );
            assertContains( changesWithValues44.getRemoved(), 44L );
        }

        @Test
        void shouldSeekWhenThereAreManyEntriesWithTheSameValues()
        {
            // GIVEN (note that 44 has the same properties as 43)
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2", "42value3" )
                    .withAdded( 43L, "43value1", "43value2", "43value3" )
                    .withAdded( 44L, "43value1", "43value2", "43value3" )
                    .withAdded( 45L, "43value1", "42value2", "42value3" )
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "43value1", "43value2", "43value3" ) );
            AddedWithValuesAndRemoved changesWithValues = indexUpdatesWithValuesForRangeSeek( state, compositeIndex3properties,
                    new Value[]{stringValue( "43value1" ), stringValue( "43value2" )}, null, IndexOrder.NONE );

            // THEN
            assertContains( changes.getAdded(), 43L, 44L );
            assertContains( changesWithValues.getAdded(), nodeWithPropertyValues( 43L, "43value1", "43value2", "43value3" ),
                    nodeWithPropertyValues( 44L, "43value1", "43value2", "43value3" ) );
        }

        @Test
        void shouldSeekInComplexMix()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 10L, "hi", 3 )
                    .withAdded( 11L, 9L, 33L )
                    .withAdded( 12L, "sneaker", false )
                    .withAdded( 13L, new int[]{10, 100}, "array-buddy" )
                    .withAdded( 14L, 40.1, 40.2 )
                    .build();

            // THEN
            assertContains( indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "hi", 3 ) ).getAdded(), 10L );
            assertContains( indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( 9L, 33L ) ).getAdded(), 11L );
            assertContains( indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "sneaker", false ) ).getAdded(), 12L );
            assertContains( indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( new int[]{10, 100}, "array-buddy" ) ).getAdded(), 13L );
            assertContains( indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( 40.1, 40.2 ) ).getAdded(), 14L );

            assertContains( indexUpdatesWithValuesForRangeSeek( state, compositeIndex,
                    new Value[]{stringValue( "hi" )}, null, IndexOrder.NONE ).getAdded(),
                    nodeWithPropertyValues( 10L, "hi", 3 ) );
            assertContains( indexUpdatesWithValuesForRangeSeek( state, compositeIndex,
                    new Value[]{longValue( 9L )}, null, IndexOrder.NONE ).getAdded(),
                    nodeWithPropertyValues( 11L, 9L, 33L ) );
            assertContains( indexUpdatesWithValuesForRangeSeek( state, compositeIndex,
                    new Value[]{stringValue( "sneaker" )}, null, IndexOrder.NONE ).getAdded(),
                    nodeWithPropertyValues( 12L, "sneaker", false ) );
            assertContains( indexUpdatesWithValuesForRangeSeek( state, compositeIndex,
                    new Value[]{intArray( new int[]{10, 100} )}, null, IndexOrder.NONE ).getAdded(),
                    nodeWithPropertyValues( 13L, new int[]{10, 100}, "array-buddy" ) );
            assertContains( indexUpdatesWithValuesForRangeSeek( state, compositeIndex,
                    new Value[]{doubleValue( 40.1 )}, null, IndexOrder.NONE ).getAdded(),
                    nodeWithPropertyValues( 14L, 40.1, 40.2 ) );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeek()
        {
            assertRangeSeekForOrder( IndexOrder.NONE );
            assertRangeSeekForOrder( IndexOrder.ASCENDING );
            assertRangeSeekForOrder( IndexOrder.DESCENDING );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefix()
        {
            assertRangeSeekByPrefixForOrder( IndexOrder.NONE );
            assertRangeSeekByPrefixForOrder( IndexOrder.ASCENDING );
            assertRangeSeekByPrefixForOrder( IndexOrder.DESCENDING );
        }

        private void assertRangeSeekForOrder( IndexOrder indexOrder )
        {
            final ReadableTransactionState state =
                    new TxStateBuilder()
                            .withAdded( 42L, 520, "random42" )
                            .withAdded( 43L, 510, "random43" )
                            .withAdded( 44L, 550, "random44" )
                            .withAdded( 45L, 500, "random45" )
                            .withAdded( 46L, "530", "random46" )
                            .withAdded( 47L, "560", "random47" )
                            .withAdded( 48L, "540", "random48" )
                            .build();

            NodeWithPropertyValues[] expectedInt = new NodeWithPropertyValues[]{
                    nodeWithPropertyValues( 43L, 510, "random43" ),
                    nodeWithPropertyValues( 42L, 520, "random42" ),
                    nodeWithPropertyValues( 44L, 550, "random44" )
            };
            NodeWithPropertyValues[] expectedString = new NodeWithPropertyValues[]{
                    nodeWithPropertyValues( 48L, "540", "random48" ),
                    nodeWithPropertyValues( 47L, "560", "random47" )
            };

            AddedAndRemoved changesInt =
                    indexUpdatesForRangeSeek( state, compositeIndex, new Value[0],
                            IndexQuery.range( -1, intValue( 500 ), false, intValue( 600 ), false ), indexOrder );
            AddedAndRemoved changesString =
                    indexUpdatesForRangeSeek( state, compositeIndex, new Value[0],
                            IndexQuery.range( -1, stringValue( "530" ), false, null, false ), indexOrder );
            AddedWithValuesAndRemoved changesWithValuesInt =
                    indexUpdatesWithValuesForRangeSeek( state, compositeIndex, new Value[0],
                            IndexQuery.range( -1, intValue( 500 ), false, intValue( 600 ), false ), indexOrder );
            AddedWithValuesAndRemoved changesWithValuesString =
                    indexUpdatesWithValuesForRangeSeek( state, compositeIndex, new Value[0],
                            IndexQuery.range( -1, stringValue( "530" ), false, null, false ), indexOrder );

            assertContains( indexOrder, changesInt, changesWithValuesInt, expectedInt );
            assertContains( indexOrder, changesString, changesWithValuesString, expectedString );
        }

        private void assertRangeSeekByPrefixForOrder( IndexOrder indexOrder )
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 40L, "Aaron", "Bass" )
                    .withAdded( 41L, "Agatha", "Christie" )
                    .withAdded( 42L, "Andreas", "Jona" )
                    .withAdded( 43L, "Barbarella", "Fonda" )
                    .withAdded( 44L, "Andrea", "Kormos" )
                    .withAdded( 45L, "Aristotle", "Nicomachus" )
                    .withAdded( 46L, "Barbara", "Mikellen" )
                    .withAdded( 47L, "Andy", "Gallagher" )
                    .withAdded( 48L, "Cinderella", "Tremaine" )
                    .withAdded( 49L, "Andromeda", "Black" )
                    .build();

            // WHEN
            AddedAndRemoved changes = indexUpdatesForRangeSeekByPrefix( state, compositeIndex, new Value[0], stringValue( "And" ), indexOrder );
            AddedWithValuesAndRemoved changesWithValues =
                    indexUpdatesWithValuesForRangeSeekByPrefix( state, compositeIndex, new Value[0], stringValue( "And" ), indexOrder );

            NodeWithPropertyValues[] expected = {
                    nodeWithPropertyValues( 44L, "Andrea", "Kormos" ),
                    nodeWithPropertyValues( 42L, "Andreas", "Jona" ),
                    nodeWithPropertyValues( 49L, "Andromeda", "Black" ),
                    nodeWithPropertyValues( 47L, "Andy", "Gallagher" )
            };

            // THEN
            assertContains( indexOrder, changes, changesWithValues, expected );
        }
    }

    private static void assertContains( IndexOrder indexOrder, AddedAndRemoved changes, AddedWithValuesAndRemoved changesWithValues,
            NodeWithPropertyValues[] expected )
    {
        if ( indexOrder == IndexOrder.DESCENDING )
        {
            ArrayUtils.reverse( expected );
        }

        long[] expectedNodeIds = Arrays.stream( expected ).mapToLong( NodeWithPropertyValues::getNodeId ).toArray();

        if ( indexOrder == IndexOrder.NONE )
        {
            assertContains( changes.getAdded(), expectedNodeIds );
            assertContains( changesWithValues.getAdded(), expected );
        }
        else
        {
            assertContainsInOrder( changes.getAdded(), expectedNodeIds );
            assertContainsInOrder( changesWithValues.getAdded(), expected );
        }
    }

    private static NodeWithPropertyValues nodeWithPropertyValues( long nodeId, Object... values )
    {
        return new NodeWithPropertyValues( nodeId, Arrays.stream( values ).map( ValueUtils::of ).toArray( Value[]::new ) );
    }

    private static void assertContains( LongIterable iterable, long... nodeIds )
    {
        assertEquals( newSetWith( nodeIds ), LongSets.immutable.ofAll( iterable ) );
    }

    private static void assertContains( Iterable<NodeWithPropertyValues> iterable, NodeWithPropertyValues... expected )
    {
        assertEquals( UnifiedSet.newSetWith( expected ), UnifiedSet.newSet( iterable ) );
    }

    private static void assertContainsInOrder( LongIterable iterable, long... nodeIds )
    {
        assertTrue( iterable.containsAll( nodeIds ), "Expected: " + iterable + " to contains: " + Arrays.toString( nodeIds ) );
    }

    private static void assertContainsInOrder( Iterable<NodeWithPropertyValues> iterable, NodeWithPropertyValues... expected )
    {
        if ( expected.length == 0 )
        {
            assertThat( iterable ).isEmpty();
        }
        else
        {
            assertThat( iterable ).containsExactly( expected );
        }
    }

    private static class TxStateBuilder
    {
        Map<ValueTuple, MutableLongDiffSetsImpl> updates = new HashMap<>();

        TxStateBuilder withAdded( long id, Object... value )
        {
            final ValueTuple valueTuple = ValueTuple.of( (Object[]) value );
            final MutableLongDiffSetsImpl changes = updates.computeIfAbsent( valueTuple,
                    ignore -> MutableLongDiffSetsImpl.createMutableLongDiffSetsImpl( OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE ) );
            changes.add( id );
            return this;
        }

        TxStateBuilder withRemoved( long id, Object... value )
        {
            final ValueTuple valueTuple = ValueTuple.of( (Object[]) value );
            final MutableLongDiffSetsImpl changes = updates.computeIfAbsent( valueTuple,
                    ignore -> MutableLongDiffSetsImpl.createMutableLongDiffSetsImpl( OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE ) );
            changes.remove( id );
            return this;
        }

        ReadableTransactionState build()
        {
            final ReadableTransactionState mock = Mockito.mock( ReadableTransactionState.class );
            doReturn( new UnmodifiableMap<>( updates ) ).when( mock ).getIndexUpdates( any( SchemaDescriptor.class ) );
            final TreeMap<ValueTuple, MutableLongDiffSetsImpl> sortedMap = new TreeMap<>( ValueTuple.COMPARATOR );
            sortedMap.putAll( updates );
            doReturn( sortedMap ).when( mock ).getSortedIndexUpdates( any( SchemaDescriptor.class ) );
            return mock;
        }
    }

}
