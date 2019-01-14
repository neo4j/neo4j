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
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.function.Predicates;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.util.collection.CachingOffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.collection.OffHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSets;
import org.neo4j.kernel.impl.util.diffsets.MutableLongDiffSetsImpl;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.txstate.DiffSets;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Pair.of;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "unchecked" )
@RunWith( Parameterized.class )
public class TxStateTest
{
    private static final CachingOffHeapBlockAllocator BLOCK_ALLOCATOR = new CachingOffHeapBlockAllocator();

    public final RandomRule random = new RandomRule();

    @Rule
    public final TestRule repeatWithDifferentRandomization()
    {
        return RuleChain.outerRule( new RepeatRule() ).around( random );
    }

    private final IndexDescriptor indexOn_1_1 = TestIndexDescriptorFactory.forLabel( 1, 1 );
    private final IndexDescriptor indexOn_2_1 = TestIndexDescriptorFactory.forLabel( 2, 1 );

    private CollectionsFactory collectionsFactory;
    private TxState state;

    @Parameter
    public CollectionsFactorySupplier collectionsFactorySupplier;

    @Parameters( name = "{0}" )
    public static List<CollectionsFactorySupplier> data()
    {
        return asList( new CollectionsFactorySupplier()
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
        }, new CollectionsFactorySupplier()
        {
            @Override
            public CollectionsFactory create()
            {
                return new OffHeapCollectionsFactory( BLOCK_ALLOCATOR );
            }

            @Override
            public String toString()
            {
                return "Off heap";
            }
        } );
    }

    @AfterClass
    public static void afterAll()
    {
        BLOCK_ALLOCATOR.release();
    }

    @Before
    public void before()
    {
        collectionsFactory = spy( collectionsFactorySupplier.create() );
        state = new TxState( collectionsFactory );
    }

    @After
    public void after()
    {
        collectionsFactory.release();
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
        LongSet addedLabels = state.nodeStateLabelDiffSets( 1 ).getAdded();

        // THEN
        assertEquals( newSetWith( 1, 2 ), addedLabels );
    }

    @Test
    public void shouldGetRemovedLabels()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        LongSet removedLabels = state.nodeStateLabelDiffSets( 1 ).getRemoved();

        // THEN
        assertEquals( newSetWith( 1, 2 ), removedLabels );
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
        assertEquals( newSetWith( 2 ), state.nodeStateLabelDiffSets( 1 ).getAdded() );
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
        assertEquals( newSetWith( 2 ), state.nodeStateLabelDiffSets( 1 ).getRemoved() );
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
        LongSet nodes = state.nodesWithLabelChanged( 2 ).getRemoved();

        // THEN
        assertEquals( newSetWith( 0L, 2L ), nodes );
    }

    //endregion

    //region index updates

    @Test
    public void shouldComputeIndexUpdatesOnUninitializedTxState()
    {
        // WHEN
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> diffSets = state.getIndexUpdates( indexOn_1_1.schema() );

        // THEN
        assertNull( diffSets );
    }

    @Test
    public void shouldComputeSortedIndexUpdatesOnUninitializedTxState()
    {
        // WHEN
        NavigableMap<ValueTuple,? extends LongDiffSets> diffSets = state.getSortedIndexUpdates( indexOn_1_1.schema() );

        // THEN
        assertNull( diffSets );
    }

    @Test
    public void shouldComputeIndexUpdatesOnEmptyTxState()
    {
        // GIVEN
        addNodesToIndex( indexOn_2_1 ).withDefaultStringProperties( 42L );

        // WHEN
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> diffSets = state.getIndexUpdates( indexOn_1_1.schema() );

        // THEN
        assertNull( diffSets );
    }

    @Test
    public void shouldComputeSortedIndexUpdatesOnEmptyTxState()
    {
        // GIVEN
        addNodesToIndex( indexOn_2_1 ).withDefaultStringProperties( 42L );

        // WHEN
        NavigableMap<ValueTuple,? extends LongDiffSets> diffSets = state.getSortedIndexUpdates( indexOn_1_1.schema() );

        // THEN
        assertNull( diffSets );
    }

    @Test
    public void shouldComputeIndexUpdatesOnTxStateWithAddedNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 41L );

        // WHEN
        UnmodifiableMap<ValueTuple,? extends LongDiffSets> diffSets = state.getIndexUpdates( indexOn_1_1.schema() );

        // THEN
        assertEqualDiffSets( addedNodes( 42L ), diffSets.get( ValueTuple.of( stringValue( "value42" ) ) ) );
        assertEqualDiffSets( addedNodes( 43L ), diffSets.get( ValueTuple.of( stringValue( "value43" ) ) ) );
        assertEqualDiffSets( addedNodes( 41L ), diffSets.get( ValueTuple.of( stringValue( "value41" ) ) ) );
    }

    @Test
    public void shouldComputeSortedIndexUpdatesOnTxStateWithAddedNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 41L );

        // WHEN
        NavigableMap<ValueTuple,? extends LongDiffSets> diffSets = state.getSortedIndexUpdates( indexOn_1_1.schema() );

        TreeMap<ValueTuple,LongDiffSets> expected = sortedAddedNodesDiffSets( 42, 41, 43 );
        // THEN
        assertEquals( expected.keySet(), diffSets.keySet() );
        for ( final ValueTuple key : expected.keySet() )
        {
            assertEqualDiffSets( expected.get( key ), diffSets.get( key ) );
        }
    }

    // endregion

    //region index rule tests

    @Test
    public void shouldAddAndGetByLabel()
    {
        // WHEN
        state.indexDoAdd( indexOn_1_1 );
        state.indexDoAdd( indexOn_2_1 );

        // THEN
        assertEquals( asSet( indexOn_1_1 ), state.indexDiffSetsByLabel( indexOn_1_1.schema().keyId() ).getAdded() );
    }

    @Test
    public void shouldAddAndGetByRuleId()
    {
        // GIVEN
        state.indexDoAdd( indexOn_1_1 );

        // THEN
        assertEquals( asSet( indexOn_1_1 ), state.indexChanges().getAdded() );
    }

    // endregion

    //region miscellaneous

    @Test
    public void shouldListNodeAsDeletedIfItIsDeleted()
    {
        // Given

        // When
        long nodeId = 1337L;
        state.nodeDoDelete( nodeId );

        // Then
        assertThat( state.addedAndRemovedNodes().getRemoved(), equalTo( newSetWith( nodeId ) ) );
    }

    @Test
    public void shouldAddUniquenessConstraint()
    {
        // when
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint, 7 );

        // then
        DiffSets<ConstraintDescriptor> diff = state.constraintsChangesForLabel( 1 );

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
        assertEquals( singleton( constraint1 ), state.constraintsChangesForSchema( constraint1.schema() ).getAdded() );
        assertEquals( singleton( constraint2 ), state.constraintsChangesForSchema( constraint2.schema() ).getAdded() );
        assertEquals( singleton( constraint3 ), state.constraintsChangesForRelationshipType( 3 ).getAdded() );
        assertEquals( singleton( constraint3 ), state.constraintsChangesForSchema( constraint3.schema() ).getAdded() );
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
    public void doNotVisitNotModifiedPropertiesOnModifiedNodes() throws ConstraintValidationException, CreateConstraintFailureException
    {
        state.nodeDoAddLabel( 5, 1 );
        MutableBoolean labelsChecked = new MutableBoolean();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
            {
                labelsChecked.setTrue();
                assertEquals( 1, id );
                assertEquals( 1, added.size() );
                assertTrue( added.contains( 5 ) );
                assertTrue( removed.isEmpty() );
            }

            @Override
            public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
            {
                fail( "Properties were not changed." );
            }
        } );
        assertTrue( labelsChecked.booleanValue() );
    }

    @Test
    public void doNotVisitNotModifiedLabelsOnModifiedNodes() throws ConstraintValidationException, CreateConstraintFailureException
    {
        state.nodeDoAddProperty( 1, 2, stringValue( "propertyValue" ) );
        MutableBoolean propertiesChecked = new MutableBoolean();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
            {
                fail( "Labels were not changed." );
            }

            @Override
            public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
            {
                propertiesChecked.setTrue();
                assertEquals( 1, id );
                assertFalse( changed.hasNext() );
                assertTrue( removed.isEmpty() );
                assertEquals( 1, Iterators.count( added, Predicates.alwaysTrue() ) );
            }
        } );
        assertTrue( propertiesChecked.booleanValue() );
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

//    getOrCreateLabelStateNodeDiffSets

    @Test
    public void getOrCreateNodeState_props_useCollectionsFactory()
    {
        final NodeStateImpl nodeState = state.getOrCreateNodeState( 1 );

        nodeState.addProperty( 2, stringValue( "foo" ) );
        nodeState.removeProperty( 3 );
        nodeState.changeProperty( 4, stringValue( "bar" ) );

        verify( collectionsFactory, times( 2 ) ).newValuesMap();
        verify( collectionsFactory, times( 1 ) ).newLongSet();
        verifyNoMoreInteractions( collectionsFactory );
    }

    @Test
    public void getOrCreateGraphState_useCollectionsFactory()
    {
        final GraphStateImpl graphState = state.getOrCreateGraphState();

        graphState.addProperty( 2, stringValue( "foo" ) );
        graphState.removeProperty( 3 );
        graphState.changeProperty( 4, stringValue( "bar" ) );

        verify( collectionsFactory, times( 2 ) ).newValuesMap();
        verify( collectionsFactory, times( 1 ) ).newLongSet();

        verifyNoMoreInteractions( collectionsFactory );
    }

    @Test
    public void getOrCreateLabelStateNodeDiffSets_useCollectionsFactory()
    {
        final MutableLongDiffSets diffSets = state.getOrCreateLabelStateNodeDiffSets(1);

        diffSets.add( 1 );
        diffSets.remove( 2 );

        verify( collectionsFactory, times( 2 ) ).newLongSet();
        verifyNoMoreInteractions( collectionsFactory );
    }

    @Test
    public void getOrCreateIndexUpdatesForSeek_useCollectionsFactory()
    {
        final MutableLongDiffSets diffSets = state.getOrCreateIndexUpdatesForSeek( new HashMap<>(), ValueTuple.of( stringValue( "test" ) ) );
        diffSets.add( 1 );
        diffSets.remove( 2 );
        verify( collectionsFactory, times( 2 ) ).newLongSet();
        verifyNoMoreInteractions( collectionsFactory );
    }

    private LongDiffSets addedNodes( long... added )
    {
        return new MutableLongDiffSetsImpl( LongSets.mutable.of( added ), LongSets.mutable.empty(), collectionsFactory );
    }

    private TreeMap<ValueTuple,LongDiffSets> sortedAddedNodesDiffSets( long... added )
    {
        TreeMap<ValueTuple,LongDiffSets> map = new TreeMap<>( ValueTuple.COMPARATOR );
        for ( long node : added )
        {

            map.put( ValueTuple.of( stringValue( "value" + node ) ), addedNodes( node ) );
        }
        return map;
    }

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
    }

    private IndexUpdater addNodesToIndex( final IndexDescriptor descriptor )
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
                withProperties( entries );
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
                    state.indexDoUpdateEntry( descriptor.schema(), nodeId, null, ValueTuple.of( valueAfter ) );
                }
            }
        };
    }

    private static void assertEqualDiffSets( LongDiffSets expected, LongDiffSets actual )
    {
        assertEquals( expected.getRemoved(), actual.getRemoved() );
        assertEquals( expected.getAdded(), actual.getAdded() );
    }
}
