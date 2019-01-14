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
package org.neo4j.kernel.impl.api.state;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.collection.primitive.Primitive.intObjectMap;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.toList;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.toSet;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Pair.of;
import static org.neo4j.kernel.impl.api.state.StubCursors.cursor;
import static org.neo4j.kernel.impl.api.state.StubCursors.relationship;
import static org.neo4j.values.storable.ValueGroup.TEXT;
import static org.neo4j.values.storable.Values.NO_VALUE;

@RunWith( Parameterized.class )
public class TxStateTest
{
    public final RandomRule random = new RandomRule();

    @Rule
    public final TestRule repeatWithDifferentRandomization()
    {
        return RuleChain.outerRule( new RepeatRule() ).around( random );
    }

    private final SchemaIndexDescriptor indexOn_1_1 = SchemaIndexDescriptorFactory.forLabel( 1, 1 );
    private final SchemaIndexDescriptor indexOn_1_2 = SchemaIndexDescriptorFactory.forLabel( 1, 2 );
    private final SchemaIndexDescriptor indexOn_2_1 = SchemaIndexDescriptorFactory.forLabel( 2, 1 );

    private CollectionsFactory collectionsFactory;
    private TxState state;

    @Parameter
    public CollectionsFactorySupplier collectionsFactorySupplier;

    @Parameters( name = "{0}" )
    public static List<CollectionsFactorySupplier> data()
    {
        return asList(
                new CollectionsFactorySupplier()
                {
                    @Override
                    public CollectionsFactory create()
                    {
                        return CollectionsFactorySupplier.ON_HEAP.create();
                    }

                    @Override
                    public String toString()
                    {
                        return "On heap";
                    }
                },
                new CollectionsFactorySupplier()
                {
                    @Override
                    public CollectionsFactory create()
                    {
                        return CollectionsFactorySupplier.OFF_HEAP.create();
                    }

                    @Override
                    public String toString()
                    {
                        return "Off heap";
                    }
                }
        );
    }

    @Before
    public void before()
    {
        collectionsFactory = collectionsFactorySupplier.create();
        state = new TxState( collectionsFactory );
    }

    @After
    public void after()
    {
        state.release();
        assertEquals( "Seems like native memory is leaking", 0L, collectionsFactory.getMemoryTracker().usedDirectMemory() );
    }

    //region node label update tests

    @Test
    public void shouldGetAddedLabels()
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        Set<Integer> addedLabels = state.nodeStateLabelDiffSets( 1 ).getAdded();

        // THEN
        assertEquals( asSet( 1, 2 ), addedLabels );
    }

    @Test
    public void shouldGetRemovedLabels()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        Set<Integer> removedLabels = state.nodeStateLabelDiffSets( 1 ).getRemoved();

        // THEN
        assertEquals( asSet( 1, 2 ), removedLabels );
    }

    @Test
    public void removeAddedLabelShouldRemoveFromAdded()
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        state.nodeDoRemoveLabel( 1, 1 );

        // THEN
        assertEquals( asSet( 2 ), state.nodeStateLabelDiffSets( 1 ).getAdded() );
    }

    @Test
    public void addRemovedLabelShouldRemoveFromRemoved()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        state.nodeDoAddLabel( 1, 1 );

        // THEN
        assertEquals( asSet( 2 ), state.nodeStateLabelDiffSets( 1 ).getRemoved() );
    }

    @Test
    public void shouldHandleMultipleLabels()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 2, 1 );
        state.nodeDoRemoveLabel( 3, 2 );
        state.nodeDoAddLabel( 1, 3 );
        state.nodeDoAddLabel( 2, 4 );
        state.nodeDoAddLabel( 3, 5 );

        // WHEN
        Set<Long> removed = state.nodesWithAllLabelsChanged( 1, 2, 3 ).getRemoved();
        Set<Long> added = state.nodesWithAllLabelsChanged( 1, 2, 3 ).getAdded();

        // THEN
        assertEquals( asSet( 0L, 1L, 2L ), Iterables.asSet( removed ) );
        assertEquals( asSet( 3L, 4L, 5L ), Iterables.asSet( added ) );
    }

    @Test
    public void shouldMapFromRemovedLabelToNodes()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 2, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 3, 1 );
        state.nodeDoRemoveLabel( 2, 2 );

        // WHEN
        Set<Long> nodes = state.nodesWithLabelChanged( 2 ).getRemoved();

        // THEN
        assertEquals( asSet( 0L, 2L ), Iterables.asSet( nodes ) );
    }

    //endregion

    //region index rule tests

    @Test
    public void shouldAddAndGetByLabel()
    {
        // WHEN
        state.indexRuleDoAdd( indexOn_1_1, null );
        state.indexRuleDoAdd( indexOn_2_1, null );

        // THEN
        assertEquals( asSet( indexOn_1_1 ),
                state.indexDiffSetsByLabel( indexOn_1_1.schema().keyId() ).getAdded() );
    }

    @Test
    public void shouldAddAndGetByRuleId()
    {
        // GIVEN
        state.indexRuleDoAdd( indexOn_1_1, null );

        // THEN
        assertEquals( asSet( indexOn_1_1 ), state.indexChanges().getAdded() );
    }

    @Test
    public void shouldRememberSpecificallySetIndexProviderDescriptor() throws ConstraintValidationException, CreateConstraintFailureException
    {
        // given
        IndexProvider.Descriptor specificProvider = new IndexProvider.Descriptor( "myProvider", "9.9" );
        state.indexRuleDoAdd( indexOn_1_1, specificProvider );

        // when
        AtomicReference<IndexProvider.Descriptor> visitedProviderDescriptor = new AtomicReference<>();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitAddedIndex( SchemaIndexDescriptor index, IndexProvider.Descriptor providerDescriptor )
            {
                visitedProviderDescriptor.set( providerDescriptor );
            }
        } );

        // then
        assertEquals( specificProvider, visitedProviderDescriptor.get() );
    }

    @Test
    public void shouldForgetSpecificallySetIndexProviderDescriptorOnDrop() throws ConstraintValidationException, CreateConstraintFailureException
    {
        // given
        IndexProvider.Descriptor specificProvider = new IndexProvider.Descriptor( "myProvider", "9.9" );
        state.indexRuleDoAdd( indexOn_1_1, specificProvider );
        state.indexDoDrop( indexOn_1_1 );
        state.indexRuleDoAdd( indexOn_1_1, null );

        // when
        MutableBoolean called = new MutableBoolean();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitAddedIndex( SchemaIndexDescriptor index, IndexProvider.Descriptor providerDescriptor )
            {
                assertNull( providerDescriptor );
                called.setTrue();
            }
        } );

        // then
        assertTrue( called.booleanValue() );
    }

    @Test
    public void shouldSpecificallySetIndexProviderDescriptorOnRecreate() throws ConstraintValidationException, CreateConstraintFailureException
    {
        // given
        IndexProvider.Descriptor specificProvider = new IndexProvider.Descriptor( "myProvider", "9.9" );
        IndexProvider.Descriptor specificProvider2 = new IndexProvider.Descriptor( "myOtherProvider", "7.7" );
        state.indexRuleDoAdd( indexOn_1_1, specificProvider );
        state.indexDoDrop( indexOn_1_1 );
        state.indexRuleDoAdd( indexOn_1_1, specificProvider2 );

        // when
        AtomicReference<IndexProvider.Descriptor> visitedProviderDescriptor = new AtomicReference<>();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitAddedIndex( SchemaIndexDescriptor index, IndexProvider.Descriptor providerDescriptor )
            {
                visitedProviderDescriptor.set( providerDescriptor );
            }
        } );

        // then
        assertEquals( specificProvider2, visitedProviderDescriptor.get() );
    }

    @Test
    public void shouldUseNullForUnspecifiedIndexProviderDescriptor() throws ConstraintValidationException, CreateConstraintFailureException
    {
        // given
        state.indexRuleDoAdd( indexOn_1_1, null );

        // when
        MutableBoolean called = new MutableBoolean();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitAddedIndex( SchemaIndexDescriptor index, IndexProvider.Descriptor providerDescriptor )
            {
                // then
                assertNull( providerDescriptor );
                called.setTrue();
            }
        } );

        // and then
        assertTrue( called.booleanValue() );
    }

    // endregion

    //region scan and seek index update tests

    @Test
    public void shouldComputeIndexUpdatesForScanOrSeekOnAnEmptyTxState()
    {
        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1 );

        // THEN
        assertTrue( diffSets.isEmpty() );
    }

    @Test
    public void shouldComputeIndexUpdatesForScanWhenThereAreNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1 );

        // THEN
        assertEquals( asSet( 42L, 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForSeekWhenThereAreNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForSeek( indexOn_1_1, ValueTuple.of( "value43" ) );

        // THEN
        assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
    }

    //endregion

    //region range seek by number index update tests

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNoMatchingNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 550 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 660 ), false, Values.of( 800 ), true ) );

        // THEN
        assertEquals( emptySet(), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNewNodesCreatedInSingleBatch()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 550 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 510 ), true, Values.of( 600 ), true ) );

        // THEN
        assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( singletonList( of( 42L, 500 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( singletonList( of( 43L, 550 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 510 ), true, Values.of( 600 ), true ) );

        // THEN
        assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 510 ), true, Values.of( 550 ), true ) );

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 510 ), true, Values.of( 550 ), false ) );

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 510 ), false, Values.of( 550 ), true ) );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 510 ), false, Values.of( 550 ), false ) );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, false, Values.of( 550 ), true ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, true, Values.of( 550 ), true ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, false, Values.of( 550 ), false ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, true, Values.of( 550 ), false ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 540 ), true, null, true ) );

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 540 ), true, null, false ) );

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 540 ), false, null, true ) );

        // THEN
        assertEquals( asSet( 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ),
                        of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( 540 ), false, null, false ) );

        // THEN
        assertEquals( asSet( 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithNoBounds()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1,  ValueGroup.NUMBER ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L ), toSet( diffSets.getAdded() ) );
    }

    //endregion

    //region range seek by string index update tests

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNoMatchingNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList( of( 42L, "Agatha" ), of( 43L, "Barbara" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 44L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Cindy" ), false, Values.of( "William" ), true ) );

        // THEN
        assertEquals( emptySet(), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNewNodesCreatedInSingleBatch()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList( of( 42L, "Agatha" ), of( 43L, "Barbara" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 44L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Amy" ), true, Values.of( "Cathy" ), true ) );

        // THEN
        assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( singletonList( of( 42L, "Agatha" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 44L, "Andreas" ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties( singletonList( of( 43L, "Barbara" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Amy" ), true, Values.of( "Cathy" ), true ) );

        // THEN
        assertEquals( asSet( 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Amy" ), true, Values.of( "Arwen" ), true ) );

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Amy" ), true, Values.of( "Arwen" ), false ) );

        // THEN
        assertEquals( asSet( 43L, 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Amy" ), false, Values.of( "Arwen" ), true ) );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Amy" ), false, Values.of( "Arwen" ), false ) );

        // THEN
        assertEquals( asSet( 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, false, Values.of( "Arwen" ), true ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, true, Values.of( "Arwen" ), true ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L, 48L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, false, Values.of( "Arwen" ), false ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, null, true, Values.of( "Arwen" ), false ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L, 45L, 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Arthur" ), true, null, true ) );

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Arthur" ), true, null, false ) );

        // THEN
        assertEquals( asSet( 47L, 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Arthur" ), false, null, true ) );

        // THEN
        assertEquals( asSet( 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ),
                        of( 47L, "Arthur" ), of( 48L, "Arwen" ), of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets =
                state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, Values.of( "Arthur" ), false, null, false ) );

        // THEN
        assertEquals( asSet( 48L, 49L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithNoBounds()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 )
                .withStringProperties( asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, IndexQuery.range( -1, TEXT ) );

        // THEN
        assertEquals( asSet( 42L, 43L, 44L ), toSet( diffSets.getAdded() ) );
    }

    //endregion

    //region range seek by prefix index update tests

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNoMatchingNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "eulav" );

        // THEN
        assertEquals( emptySet(), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNewNodesCreatedInOneBatch()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "value" );

        // THEN
        assertEquals( asSet( 42L, 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingNewNodes1()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ),
                        of( 44L, "Aristotle" ), of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
                        of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "And" );

        // THEN
        assertEquals( asSet( 42L, 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingNewNodes2()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ),
                        of( 44L, "Aristotle" ), of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
                        of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Bar" );

        // THEN
        assertEquals( asSet( 45L, 46L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingLeadingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ),
                        of( 44L, "Aristotle" ), of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
                        of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Aa" );

        // THEN
        assertEquals( asSet( 40L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingTrailingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ),
                        of( 44L, "Aristotle" ), of( 45L, "Barbara" ), of( 46L, "Barbarella" ),
                        of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Ci" );

        // THEN
        assertEquals( asSet( 47L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "value" );

        // THEN
        assertEquals( asSet( 42L, 43L ), toSet( diffSets.getAdded() ) );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNonStringNodes() throws Exception
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( Collections.singleton( Pair.of( 44L, 101L ) ) );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );

        // WHEN
        PrimitiveLongReadableDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "value" );

        // THEN
        assertEquals( asSet( 42L, 43L ), toSet( diffSets.getAdded() ) );
    }

    //endregion

    //region miscellaneous

    @Test
    public void shouldListNodeAsDeletedIfItIsDeleted()
    {
        // Given

        // When
        long nodeId = 1337L;
        state.nodeDoDelete( nodeId );

        // Then
        assertThat( Iterables.asSet( state.addedAndRemovedNodes().getRemoved() ), equalTo( asSet( nodeId ) ) );
    }

    @Test
    public void shouldAddUniquenessConstraint()
    {
        // when
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint, 7 );

        // then
        ReadableDiffSets<ConstraintDescriptor> diff = state.constraintsChangesForLabel( 1 );

        assertEquals( singleton( constraint ), diff.getAdded() );
        assertTrue( diff.getRemoved().isEmpty() );
    }

    @Test
    public void addingUniquenessConstraintShouldBeIdempotent()
    {
        // given
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );

        // when
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( constraint1, constraint2 );
        assertEquals( singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
    }

    @Test
    public void shouldDifferentiateBetweenUniquenessConstraintsForDifferentLabels()
    {
        // when
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 2, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
        assertEquals( singleton( constraint2 ), state.constraintsChangesForLabel( 2 ).getAdded() );
    }

    @Test
    public void shouldAddRelationshipPropertyExistenceConstraint()
    {
        // Given
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType( 1, 42 );

        // When
        state.constraintDoAdd( constraint );

        // Then
        assertEquals( singleton( constraint ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
    }

    @Test
    public void addingRelationshipPropertyExistenceConstraintConstraintShouldBeIdempotent()
    {
        // Given
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.existsForRelType( 1, 42 );
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.existsForRelType( 1, 42 );

        // When
        state.constraintDoAdd( constraint1 );
        state.constraintDoAdd( constraint2 );

        // Then
        assertEquals( constraint1, constraint2 );
        assertEquals( singleton( constraint1 ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
    }

    @Test
    public void shouldDropRelationshipPropertyExistenceConstraint()
    {
        // Given
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType( 1, 42 );
        state.constraintDoAdd( constraint );

        // When
        state.constraintDoDrop( constraint );

        // Then
        assertTrue( state.constraintsChangesForRelationshipType( 1 ).isEmpty() );
    }

    @Test
    public void shouldDifferentiateRelationshipPropertyExistenceConstraints()
    {
        // Given
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.existsForRelType( 1, 11 );
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.existsForRelType( 1, 22 );
        ConstraintDescriptor constraint3 = ConstraintDescriptorFactory.existsForRelType( 3, 33 );

        // When
        state.constraintDoAdd( constraint1 );
        state.constraintDoAdd( constraint2 );
        state.constraintDoAdd( constraint3 );

        // Then
        assertEquals( asSet( constraint1, constraint2 ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
        assertEquals( singleton( constraint1 ),
                state.constraintsChangesForSchema( constraint1.schema() ).getAdded() );
        assertEquals( singleton( constraint2 ),
                state.constraintsChangesForSchema( constraint2.schema() ).getAdded() );
        assertEquals( singleton( constraint3 ), state.constraintsChangesForRelationshipType( 3 ).getAdded() );
        assertEquals( singleton( constraint3 ),
                state.constraintsChangesForSchema( constraint3.schema() ).getAdded() );
    }

    @Test
    public void shouldListRelationshipsAsCreatedIfCreated()
    {
        // When
        long relId = 10;
        state.relationshipDoCreate( relId, 0, 1, 2 );

        // Then
        assertTrue( state.hasChanges() );
        assertTrue( state.relationshipIsAddedInThisTx( relId ) );
    }

    @Test
    public void shouldGiveCorrectDegreeWhenAddingAndRemovingRelationships()
    {
        // Given
        int startNode = 1;
        int endNode = 2;
        int relType = 0;

        // When
        state.relationshipDoCreate( 10, relType, startNode, endNode );
        state.relationshipDoCreate( 11, relType, startNode, endNode );
        state.relationshipDoCreate( 12, relType + 1, startNode, endNode );
        state.relationshipDoCreate( 13, relType + 1, endNode, startNode );

        state.relationshipDoDelete( 1337, relType, startNode, endNode );
        state.relationshipDoDelete( 1338, relType + 1, startNode, startNode );

        // Then
        assertEquals( 12, state.augmentNodeDegree( startNode, 10, Direction.BOTH ) );
        assertEquals( 10, state.augmentNodeDegree( startNode, 10, Direction.INCOMING ) );
        assertEquals( 11, state.augmentNodeDegree( startNode, 10, Direction.BOTH, relType ) );
    }

    @Test
    public void shouldGiveCorrectRelationshipTypesForNode()
    {
        // Given
        int startNode = 1;
        int endNode = 2;
        int relType = 0;

        // When
        long relA = 10;
        long relB = 11;
        long relC = 12;
        state.relationshipDoCreate( relA, relType, startNode, endNode );
        state.relationshipDoCreate( relB, relType, startNode, endNode );
        state.relationshipDoCreate( relC, relType + 1, startNode, endNode );

        state.relationshipDoDelete( relB, relType, startNode, endNode );
        state.relationshipDoDelete( relC, relType + 1, startNode, endNode );

        // Then
        assertThat( toList( state.nodeRelationshipTypes( startNode ).iterator() ), equalTo( asList( relType ) ) );
    }

    @Test
    public void shouldNotChangeRecordForCreatedAndDeletedNode() throws Exception
    {
        // GIVEN
        state.nodeDoCreate( 0 );
        state.nodeDoDelete( 0 );
        state.nodeDoCreate( 1 );

        // WHEN
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitCreatedNode( long id )
            {
                assertEquals( "Should not create any other node than 1", 1, id );
            }

            @Override
            public void visitDeletedNode( long id )
            {
                fail( "Should not delete any node" );
            }
        } );
    }

    @Test
    public void shouldVisitDeletedNode() throws Exception
    {
        // Given
        state.nodeDoDelete( 42 );

        // When
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitDeletedNode( long id )
            {
                // Then
                assertEquals( "Wrong deleted node id", 42, id );
            }
        } );
    }

    @Test
    public void shouldReportDeletedNodeIfItWasCreatedAndDeletedInSameTx()
    {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate( nodeId );
        state.nodeDoDelete( nodeId );

        // Then
        assertTrue( state.nodeIsDeletedInThisTx( nodeId ) );
    }

    @Test
    public void shouldNotReportDeletedNodeIfItIsNotDeleted()
    {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate( nodeId );

        // Then
        assertFalse( state.nodeIsDeletedInThisTx( nodeId ) );
    }

    @Test
    public void shouldNotChangeRecordForCreatedAndDeletedRelationship() throws Exception
    {
        // GIVEN
        state.relationshipDoCreate( 0, 0, 1, 2 );
        state.relationshipDoDelete( 0, 0, 1, 2 );
        state.relationshipDoCreate( 1, 0, 2, 3 );

        // WHEN
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                assertEquals( "Should not create any other relationship than 1", 1, id );
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                fail( "Should not delete any relationship" );
            }
        } );
    }

    @Test
    public void shouldVisitDeletedRelationship() throws Exception
    {
        // Given
        state.relationshipDoDelete( 42, 2, 3, 4 );

        // When
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitDeletedRelationship( long id )
            {
                // Then
                assertEquals( "Wrong deleted relationship id", 42, id );
            }
        } );
    }

    @Test
    public void shouldReportDeletedRelationshipIfItWasCreatedAndDeletedInSameTx()
    {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate( relationshipId, relationshipType, startNodeId, endNodeId );
        state.relationshipDoDelete( relationshipId, relationshipType, startNodeId, endNodeId );

        // Then
        assertTrue( state.relationshipIsDeletedInThisTx( relationshipId ) );
    }

    @Test
    public void shouldNotReportDeletedRelationshipIfItIsNotDeleted()
    {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate( relationshipId, relationshipType, startNodeId, endNodeId );

        // Then
        assertFalse( state.relationshipIsDeletedInThisTx( relationshipId ) );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitCreatedNodesBeforeDeletedNodes() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.nodeDoCreate( /*id=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.nodeDoDelete( /*id=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitCreatedNode( long id )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedNode( long id )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitCreatedNodesBeforeCreatedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.nodeDoCreate( /*id=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitCreatedNode( long id )
            {
                visitEarly();
            }

            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitCreatedRelationshipsBeforeDeletedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.relationshipDoDelete( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            // then
            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitDeletedNodesAfterDeletedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.nodeDoDelete( /*id=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitDeletedRelationship( long id )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedNode( long id )
            {
                visitLate();
            }
        } );
    }

    @Test
    public void shouldObserveCorrectAugmentedNodeRelationshipsState()
    {
        // GIVEN random committed state
        TxState state = new TxState();
        for ( int i = 0; i < 100; i++ )
        {
            state.nodeDoCreate( i );
        }
        for ( int i = 0; i < 5; i++ )
        {
            state.relationshipTypeDoCreateForName( "Type-" + i, i );
        }
        Map<Long,RelationshipItem> committedRelationships = new HashMap<>();
        long relationshipId = 0;
        int nodeCount = 100;
        int relationshipTypeCount = 5;
        for ( int i = 0; i < 30; i++ )
        {
            RelationshipItem relationship = relationship( relationshipId++, random.nextInt( relationshipTypeCount ),
                    random.nextInt( nodeCount ), random.nextInt( nodeCount ) );
            committedRelationships.put( relationship.id(), relationship );
        }
        Map<Long,RelationshipItem> allRelationships = new HashMap<>( committedRelationships );
        // and some random changes to that
        for ( int i = 0; i < 10; i++ )
        {
            if ( random.nextBoolean() )
            {
                RelationshipItem relationship = relationship( relationshipId++, random.nextInt( relationshipTypeCount ),
                        random.nextInt( nodeCount ), random.nextInt( nodeCount ) );
                allRelationships.put( relationship.id(), relationship );
                state.relationshipDoCreate( relationship.id(), relationship.type(), relationship.startNode(),
                        relationship.endNode() );
            }
            else
            {
                RelationshipItem relationship = Iterables
                        .fromEnd( committedRelationships.values(), random.nextInt( committedRelationships.size() ) );
                state.relationshipDoDelete( relationship.id(), relationship.type(), relationship.startNode(),
                        relationship.endNode() );
                allRelationships.remove( relationship.id() );
            }
        }
        // WHEN
        for ( int i = 0; i < nodeCount; i++ )
        {
            Direction direction = Direction.values()[random.nextInt( Direction.values().length )];
            int[] relationshipTypes = randomTypes( relationshipTypeCount, random.random() );
            Cursor<RelationshipItem> committed = cursor(
                    relationshipsForNode( i, committedRelationships, direction, relationshipTypes ).values() );
            Cursor<RelationshipItem> augmented = relationshipTypes == null
                                                 ? state.augmentNodeRelationshipCursor( committed, state.getNodeState( i ), direction )
                                                 : state.augmentNodeRelationshipCursor( committed, state.getNodeState( i ), direction,
                                                         relationshipTypes );

            Map<Long,RelationshipItem> expectedRelationships =
                    relationshipsForNode( i, allRelationships, direction, relationshipTypes );
            // THEN
            while ( augmented.next() )
            {
                RelationshipItem relationship = augmented.get();
                RelationshipItem actual = expectedRelationships.remove( relationship.id() );
                assertNotNull( "Augmented cursor returned relationship " + relationship + ", but shouldn't have",
                        actual );
                assertRelationshipEquals( actual, relationship );
            }
            assertTrue( "Augmented cursor didn't return some expected relationships: " + expectedRelationships,
                    expectedRelationships.isEmpty() );
        }
    }

    @Test
    public void useCollectionFactory()
    {
        final CollectionsFactory collectionsFactory = mock( CollectionsFactory.class );
        doAnswer( invocation -> intObjectMap() ).when( collectionsFactory ).newIntObjectMap();

        state = new TxState( collectionsFactory );

        state.labelDoCreateForName( "foo", 1 );
        state.propertyKeyDoCreateForName( "bar", 2 );
        state.relationshipTypeDoCreateForName( "baz", 3 );

        verify( collectionsFactory, times( 3 ) ).newIntObjectMap();
        verifyNoMoreInteractions( collectionsFactory );
    }

    private Map<Long,RelationshipItem> relationshipsForNode( long nodeId, Map<Long,RelationshipItem> allRelationships,
            Direction direction, int[] relationshipTypes )
    {
        Map<Long,RelationshipItem> result = new HashMap<>();
        for ( RelationshipItem relationship : allRelationships.values() )
        {
            switch ( direction )
            {
            case OUTGOING:
                if ( relationship.startNode() != nodeId )
                {
                    continue;
                }
                break;
            case INCOMING:
                if ( relationship.endNode() != nodeId )
                {
                    continue;
                }
                break;
            case BOTH:
                if ( relationship.startNode() != nodeId && relationship.endNode() != nodeId )
                {
                    continue;
                }
                break;
            default:
                throw new IllegalStateException( "Unknown direction: " + direction );
            }

            if ( relationshipTypes != null )
            {
                if ( !contains( relationshipTypes, relationship.type() ) )
                {
                    continue;
                }
            }

            result.put( relationship.id(), relationship );
        }
        return result;
    }

    private void assertRelationshipEquals( RelationshipItem expected, RelationshipItem relationship )
    {
        assertEquals( expected.id(), relationship.id() );
        assertEquals( expected.type(), relationship.type() );
        assertEquals( expected.startNode(), relationship.startNode() );
        assertEquals( expected.endNode(), relationship.endNode() );
    }

    private int[] randomTypes( int high, Random random )
    {
        int count = random.nextInt( high );
        if ( count == 0 )
        {
            return null;
        }
        int[] types = new int[count];
        Arrays.fill( types, -1 );
        for ( int i = 0; i < count; )
        {
            int candidate = random.nextInt( high );
            if ( !contains( types, candidate ) )
            {
                types[i++] = candidate;
            }
        }
        return types;
    }

    private boolean contains( int[] array, int candidate )
    {
        for ( int i : array )
        {
            if ( i == candidate )
            {
                return true;
            }
        }
        return false;
    }

    //endregion

    abstract class VisitationOrder extends TxStateVisitor.Adapter
    {
        private final Set<String> visitMethods = new HashSet<>();

        VisitationOrder( int size )
        {
            for ( Method method : getClass().getDeclaredMethods() )
            {
                if ( method.getName().startsWith( "visit" ) )
                {
                    visitMethods.add( method.getName() );
                }
            }
            assertEquals( "should implement exactly two visit*(...) methods", 2, visitMethods.size() );
            do
            {
                if ( random.nextBoolean() )
                {
                    createEarlyState();
                }
                else
                {
                    createLateState();
                }
            }
            while ( size-- > 0 );
        }

        abstract void createEarlyState();

        abstract void createLateState();

        private boolean late;

        final void visitEarly()
        {
            if ( late )
            {
                String early = "the early visit*-method";
                String late = "the late visit*-method";
                for ( StackTraceElement trace : Thread.currentThread().getStackTrace() )
                {
                    if ( visitMethods.contains( trace.getMethodName() ) )
                    {
                        early = trace.getMethodName();
                        for ( String method : visitMethods )
                        {
                            if ( !method.equals( early ) )
                            {
                                late = method;
                            }
                        }
                        break;
                    }
                }
                fail( early + "(...) should not be invoked after " + late + "(...)" );
            }
        }

        final void visitLate()
        {
            late = true;
        }
    }

    private interface IndexUpdater
    {
        void withDefaultStringProperties( long... nodeIds );

        void withStringProperties( Collection<Pair<Long,String>> nodesWithValues );

        <T extends Number> void withNumberProperties( Collection<Pair<Long,T>> nodesWithValues );

        void withBooleanProperties( Collection<Pair<Long,Boolean>> nodesWithValues );
    }

    private IndexUpdater addNodesToIndex( final SchemaIndexDescriptor descriptor )
    {
        return new IndexUpdater()
        {
            @Override
            public void withDefaultStringProperties( long... nodeIds )
            {
                Collection<Pair<Long,String>> entries = new ArrayList<>( nodeIds.length );
                for ( long nodeId : nodeIds )
                {
                    entries.add( of( nodeId, "value" + nodeId ) );
                }
                withStringProperties( entries );
            }

            @Override
            public void withStringProperties( Collection<Pair<Long,String>> nodesWithValues )
            {
                withProperties( nodesWithValues );
            }

            @Override
            public <T extends Number> void withNumberProperties( Collection<Pair<Long,T>> nodesWithValues )
            {
                withProperties( nodesWithValues );
            }

            @Override
            public void withBooleanProperties( Collection<Pair<Long,Boolean>> nodesWithValues )
            {
                withProperties( nodesWithValues );
            }

            private <T> void withProperties( Collection<Pair<Long,T>> nodesWithValues )
            {
                final int labelId = descriptor.schema().keyId();
                final int propertyKeyId = descriptor.schema().getPropertyId();
                for ( Pair<Long,T> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.nodeDoCreate( nodeId );
                    state.nodeDoAddLabel( labelId, nodeId );
                    Value valueAfter = Values.of( entry.other() );
                    state.nodeDoAddProperty( nodeId, propertyKeyId, valueAfter );
                    state.indexDoUpdateEntry( descriptor.schema(), nodeId, null,
                            ValueTuple.of( valueAfter ) );
                }
            }
        };
    }
}
