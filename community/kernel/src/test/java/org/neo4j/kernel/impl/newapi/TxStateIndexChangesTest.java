/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.set.primitive.LongSet;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.neo4j.collection.PrimitiveLongCollections.toSet;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.values.storable.Values.NO_VALUE;

class TxStateIndexChangesTest
{
    private final IndexDescriptor index = TestIndexDescriptorFactory.forLabel( 1, 1 );

    @Test
    void shouldComputeIndexUpdatesForScanOnAnEmptyTxState()
    {
        final ReadableTransactionState state = Mockito.mock( ReadableTransactionState.class );

        // WHEN
        LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForScan( state, index );
        DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForScan( state, index );

        // THEN
        assertTrue( diffSets.isEmpty() );
        assertTrue( diffSets2.isEmpty() );
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
        LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForScan( state, index );
        DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForScan( state, index );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( UnifiedSet.newSetWith( nodeWithPropertyValues( 42L, "foo" ), nodeWithPropertyValues( 43L, "bar" ) ), diffSets2.getAdded() );
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
        LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSeek( state, index, ValueTuple.of( "bar" ) );

        // THEN
        assertEquals( newSetWith( 43L ), diffSets.getAdded() );
    }

    @TestFactory
    Collection<DynamicTest> rangeTests()
    {
        final ReadableTransactionState state = new TxStateBuilder()
                .withAdded( 42L, 500 )
                .withAdded( 43L, 510 )
                .withAdded( 44L, 520 )
                .withAdded( 45L, 530 )
                .withAdded( 47L, 540 )
                .withAdded( 48L, 550 )
                .withAdded( 49L, 560 )
                .build();

        final Collection<DynamicTest> tests = new ArrayList<>();

        tests.add( rangeTest( state, Values.of( 510 ), true, Values.of( 550 ), true,
                nodeWithPropertyValues( 43L, 510 ),
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 ),
                nodeWithPropertyValues( 48L, 550 )
        ) );
        tests.add( rangeTest( state, Values.of( 510 ), true, Values.of( 550 ), false,
                nodeWithPropertyValues( 43L, 510 ),
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 )
        ) );
        tests.add( rangeTest( state, Values.of( 510 ), false, Values.of( 550 ), true,
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 ),
                nodeWithPropertyValues( 48L, 550 )
        ) );
        tests.add( rangeTest( state, Values.of( 510 ), false, Values.of( 550 ), false,
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 )
        ) );
        tests.add( rangeTest( state, null, false, Values.of( 550 ), true,
                nodeWithPropertyValues( 42L, 500 ),
                nodeWithPropertyValues( 43L, 510 ),
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 ),
                nodeWithPropertyValues( 48L, 550 )
        ) );
        tests.add( rangeTest( state, null, true, Values.of( 550 ), true,
                nodeWithPropertyValues( 42L, 500 ),
                nodeWithPropertyValues( 43L, 510 ),
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 ),
                nodeWithPropertyValues( 48L, 550 )
        ) );
        tests.add( rangeTest( state, null, false, Values.of( 550 ), false,
                nodeWithPropertyValues( 42L, 500 ),
                nodeWithPropertyValues( 43L, 510 ),
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 )
        ) );
        tests.add( rangeTest( state, null, true, Values.of( 550 ), false,
                nodeWithPropertyValues( 42L, 500 ),
                nodeWithPropertyValues( 43L, 510 ),
                nodeWithPropertyValues( 44L, 520 ),
                nodeWithPropertyValues( 45L, 530 ),
                nodeWithPropertyValues( 47L, 540 )
        ) );
        tests.add( rangeTest( state, Values.of( 540 ), true, null, true,
                nodeWithPropertyValues( 47L, 540 ),
                nodeWithPropertyValues( 48L, 550 ),
                nodeWithPropertyValues( 49L, 560 )
        ) );
        tests.add( rangeTest( state, Values.of( 540 ), true, null, false,
                nodeWithPropertyValues( 47L, 540 ),
                nodeWithPropertyValues( 48L, 550 ),
                nodeWithPropertyValues( 49L, 560 )
        ) );
        tests.add( rangeTest( state, Values.of( 540 ), false, null, true,
                nodeWithPropertyValues( 48L, 550 ),
                nodeWithPropertyValues( 49L, 560 )
        ) );
        tests.add( rangeTest( state, Values.of( 540 ), false, null, false,
                nodeWithPropertyValues( 48L, 550 ),
                nodeWithPropertyValues( 49L, 560 )
        ) );
        tests.add( rangeTest( state, Values.of( 560 ), false, Values.of( 800 ), true ) );

        return tests;
    }

    private DynamicTest rangeTest( ReadableTransactionState state, Value lo, boolean includeLo, Value hi, boolean includeHi,
            NodeWithPropertyValues... expected )
    {
        return DynamicTest.dynamicTest( String.format( "range seek: lo=%s (incl: %s), hi=%s (incl: %s)", lo, includeLo, hi, includeHi ), () ->
        {
            // Internal production code relies on null for unbounded, and cannot cope with NO_VALUE in this case
            assert lo != NO_VALUE;
            assert hi != NO_VALUE;
            final LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForRangeSeek( state, index, IndexQuery.range( -1, lo, includeLo, hi, includeHi ) );
            final DiffSets<NodeWithPropertyValues> diffSets2 =
                    TxStateIndexChanges.indexUpdatesWithValuesForRangeSeek( state, index, IndexQuery.range( -1, lo, includeLo, hi, includeHi ) );

            final LongSet expectedNodeIds = LongSets.immutable.ofAll( Arrays.stream( expected ).mapToLong( NodeWithPropertyValues::getNodeId ) );
            assertEquals( expectedNodeIds, diffSets.getAdded() );
            assertEquals( UnifiedSet.newSetWith( expected ), diffSets2.getAdded() );
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
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSuffixOrContains( state, index,
                    IndexQuery.stringContains( index.schema().getPropertyId(), "eulav" ) );
            DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains( state, index,
                    IndexQuery.stringContains( index.schema().getPropertyId(), "eulav" ) );

            // THEN
            assertEquals( 0, diffSets.getAdded().size() );
            assertEquals( 0, diffSets2.getAdded().size() );
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
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSuffixOrContains( state, index,
                    IndexQuery.stringSuffix( index.schema().getPropertyId(), "ella" ) );
            DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains( state, index,
                    IndexQuery.stringSuffix( index.schema().getPropertyId(), "ella" ) );

            // THEN
            assertEquals( newSetWith( 46L, 47L ), diffSets.getAdded() );
            assertEquals(
                    UnifiedSet.newSetWith(
                            nodeWithPropertyValues( 46L, "Barbarella" ),
                            nodeWithPropertyValues( 47L, "Cinderella" ) ),
                    diffSets2.getAdded() );
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
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSuffixOrContains( state, index,
                    IndexQuery.stringContains( index.schema().getPropertyId(), "arbar" ) );
            DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForSuffixOrContains( state, index,
                    IndexQuery.stringContains( index.schema().getPropertyId(), "arbar" ) );

            // THEN
            assertEquals( newSetWith( 45L, 46L ), diffSets.getAdded() );
            assertEquals(
                    UnifiedSet.newSetWith(
                            nodeWithPropertyValues( 45L, "Barbara" ),
                            nodeWithPropertyValues( 46L, "Barbarella" ) ),
                    diffSets2.getAdded() );
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
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix( state, index, "eulav" );
            DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix( state, index, "eulav" );

            // THEN
            assertEquals( 0, diffSets.getAdded().size() );
            assertEquals( 0, diffSets2.getAdded().size() );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingNewNodes()
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
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix( state, index, "And" );
            DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForRangeSeekByPrefix( state, index, "And" );

            // THEN
            assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
            assertEquals(
                    UnifiedSet.newSetWith(
                            nodeWithPropertyValues( 42L, "Andreas" ),
                            nodeWithPropertyValues( 43L, "Andrea" ) ),
                    diffSets2.getAdded() );
        }

        @Test
        void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNonStringNodes() throws Exception
        {
            // GIVEN
            final ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "barry" )
                    .withAdded( 44L, 101L )
                    .withAdded( 43L, "bar" )
                    .build();

            // WHEN
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForRangeSeekByPrefix( state, index, "bar" );

            // THEN
            assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        }
    }

    @Nested
    class CompositeIndex
    {
        private final IndexDescriptor compositeIndex = TestIndexDescriptorFactory.forLabel( 1, 1, 2 );

        @Test
        void shouldSeekOnAnEmptyTxState()
        {
            // GIVEN
            final ReadableTransactionState state = Mockito.mock( ReadableTransactionState.class );

            // WHEN
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "43value1", "43value2" ) );

            // THEN
            assertTrue( diffSets.isEmpty() );
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
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForScan( state, compositeIndex );
            DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForScan( state, compositeIndex );

            // THEN
            assertEquals( asSet( 42L, 43L ), toSet( diffSets.getAdded() ) );
            assertEquals(
                    UnifiedSet.newSetWith(
                            nodeWithPropertyValues( 42L, "42value1", "42value2" ),
                            nodeWithPropertyValues( 43L, "43value1", "43value2" ) ),
                    diffSets2.getAdded() );
        }

        @Test
        void shouldSeekWhenThereAreNewStringNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2" )
                    .withAdded( 43L, "43value1", "43value2" )
                    .build();

            // WHEN
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "43value1", "43value2" ) );

            // THEN
            assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
        }

        @Test
        void shouldSeekWhenThereAreNewNumberNodes()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, 42001.0, 42002.0 )
                    .withAdded( 43L, 43001.0, 43002.0 )
                    .build();

            // WHEN
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( 43001.0, 43002.0 ) );

            // THEN
            assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
        }

        @Test
        void shouldHandleMixedAddsAndRemovesEntry()
        {
            // GIVEN
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2" )
                    .withAdded( 43L, "43value1", "43value2" )
                    .withRemoved( 43L, "43value1", "43value2" )
                    .withRemoved( 44L, "44value1", "44value2" )
                    .build();

            // WHEN
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForScan( state, compositeIndex );
            DiffSets<NodeWithPropertyValues> diffSets2 = TxStateIndexChanges.indexUpdatesWithValuesForScan( state, compositeIndex );

            // THEN
            assertEquals( newSetWith( 42L ), diffSets.getAdded() );
            assertEquals( UnifiedSet.newSetWith( nodeWithPropertyValues( 42L, "42value1", "42value2" ) ), diffSets2.getAdded() );
            assertEquals( newSetWith( 44L ), diffSets.getRemoved() );
            assertEquals( UnifiedSet.newSetWith( nodeWithPropertyValues( 44L, "44value1", "44value2" ) ), diffSets2.getRemoved() );
        }

        @Test
        void shouldSeekWhenThereAreManyEntriesWithTheSameValues()
        {
            // GIVEN (note that 44 has the same properties as 43)
            ReadableTransactionState state = new TxStateBuilder()
                    .withAdded( 42L, "42value1", "42value2" )
                    .withAdded( 43L, "43value1", "43value2" )
                    .withAdded( 44L, "43value1", "43value2" )
                    .build();

            // WHEN
            LongDiffSets diffSets = TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "43value1", "43value2" ) );

            // THEN
            assertEquals( asSet( 43L, 44L ), toSet( diffSets.getAdded() ) );
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
            assertEquals( asSet( 10L ),
                    toSet( TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "hi", 3 ) ).getAdded() ) );
            assertEquals( asSet( 11L ),
                    toSet( TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( 9L, 33L ) ).getAdded() ) );
            assertEquals( asSet( 12L ),
                    toSet( TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( "sneaker", false ) ).getAdded() ) );
            assertEquals( asSet( 13L ),
                    toSet( TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( new int[]{10, 100}, "array-buddy" ) ).getAdded() ) );
            assertEquals( asSet( 14L ),
                    toSet( TxStateIndexChanges.indexUpdatesForSeek( state, compositeIndex, ValueTuple.of( 40.1, 40.2 ) ).getAdded() ) );
        }

    }

    private static NodeWithPropertyValues nodeWithPropertyValues( long nodeId, Object... values )
    {
        return new NodeWithPropertyValues( nodeId, Arrays.stream( values ).map( ValueUtils::of ).toArray( Value[]::new ) );
    }

    private static class TxStateBuilder
    {
        Map<ValueTuple, MutableLongDiffSetsImpl> updates = new HashMap<>();

        TxStateBuilder withAdded( long id, Object... value )
        {
            final ValueTuple valueTuple = ValueTuple.of( (Object[]) value );
            final MutableLongDiffSetsImpl diffSets = updates.computeIfAbsent( valueTuple, ignore -> new MutableLongDiffSetsImpl() );
            diffSets.add( id );
            return this;
        }

        TxStateBuilder withRemoved( long id, Object... value )
        {
            final ValueTuple valueTuple = ValueTuple.of( (Object[]) value );
            final MutableLongDiffSetsImpl diffSets = updates.computeIfAbsent( valueTuple, ignore -> new MutableLongDiffSetsImpl() );
            diffSets.remove( id );
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
