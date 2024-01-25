/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.state;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Pair.of;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.values.storable.Values.stringValue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.collection.diffset.DiffSets;
import org.neo4j.collection.diffset.LongDiffSets;
import org.neo4j.collection.diffset.MutableLongDiffSets;
import org.neo4j.collection.diffset.MutableLongDiffSetsImpl;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransactionSink;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
abstract class TxStateTest {
    @Inject
    private RandomSupport random;

    private final IndexDescriptor indexOn_1_1 = TestIndexDescriptorFactory.forLabel(1, 1);
    private final IndexDescriptor indexOn_2_1 = TestIndexDescriptorFactory.forLabel(2, 1);
    private final IndexDescriptor indexOnRels =
            TestIndexDescriptorFactory.forSchema(SchemaDescriptors.forRelType(3, 1));
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private CollectionsFactory collectionsFactory;
    private TxState state;
    MemoryTracker memoryTracker;

    TxStateTest(CollectionsFactorySupplier collectionsFactorySupplier) {
        this.collectionsFactorySupplier = collectionsFactorySupplier;
    }

    @BeforeEach
    void before() {
        memoryTracker = new LocalMemoryTracker();
        collectionsFactory = spy(collectionsFactorySupplier.create());
        state = new TxState(
                collectionsFactory,
                memoryTracker,
                TransactionStateBehaviour.DEFAULT_BEHAVIOUR,
                ApplyEnrichmentStrategy.NO_ENRICHMENT,
                ChunkedTransactionSink.EMPTY,
                TransactionEvent.NULL);
    }

    @AfterEach
    void after() {
        collectionsFactory.release();
        assertEquals(0L, memoryTracker.usedNativeMemory(), "Seems like native memory is leaking");
    }

    abstract long usedMemory();

    @Test
    void shouldGetAddedLabels() {
        // GIVEN
        state.nodeDoAddLabel(1, 0);
        state.nodeDoAddLabel(1, 1);
        state.nodeDoAddLabel(2, 1);

        // WHEN
        LongSet addedLabels = state.nodeStateLabelDiffSets(1).getAdded();

        // THEN
        assertEquals(newSetWith(1, 2), addedLabels);
    }

    @Test
    void shouldGetRemovedLabels() {
        // GIVEN
        state.nodeDoRemoveLabel(1, 0);
        state.nodeDoRemoveLabel(1, 1);
        state.nodeDoRemoveLabel(2, 1);

        // WHEN
        LongSet removedLabels = state.nodeStateLabelDiffSets(1).getRemoved();

        // THEN
        assertEquals(newSetWith(1, 2), removedLabels);
    }

    @Test
    void removeAddedLabelShouldRemoveFromAdded() {
        // GIVEN
        state.nodeDoAddLabel(1, 0);
        state.nodeDoAddLabel(1, 1);
        state.nodeDoAddLabel(2, 1);

        // WHEN
        state.nodeDoRemoveLabel(1, 1);

        // THEN
        assertEquals(newSetWith(2), state.nodeStateLabelDiffSets(1).getAdded());
    }

    @Test
    void addRemovedLabelShouldRemoveFromRemoved() {
        // GIVEN
        state.nodeDoRemoveLabel(1, 0);
        state.nodeDoRemoveLabel(1, 1);
        state.nodeDoRemoveLabel(2, 1);

        // WHEN
        state.nodeDoAddLabel(1, 1);

        // THEN
        assertEquals(newSetWith(2), state.nodeStateLabelDiffSets(1).getRemoved());
    }

    @Test
    void shouldMapFromRemovedLabelToNodes() {
        // GIVEN
        state.nodeDoRemoveLabel(1, 0);
        state.nodeDoRemoveLabel(2, 0);
        state.nodeDoRemoveLabel(1, 1);
        state.nodeDoRemoveLabel(3, 1);
        state.nodeDoRemoveLabel(2, 2);

        // WHEN
        LongSet nodes = state.nodesWithLabelChanged(2).getRemoved();

        // THEN
        assertEquals(newSetWith(0L, 2L), nodes);
    }

    @Test
    void shouldGetAddedRelationshipsByType() {
        // GIVEN
        state.relationshipDoCreate(1, 1, 1, 1);
        state.relationshipDoCreate(2, 1, 1, 1);
        state.relationshipDoCreate(3, 2, 1, 1);

        // WHEN
        LongSet addedRelationshipsWithType =
                state.relationshipsWithTypeChanged(1).getAdded();

        // THEN
        assertEquals(newSetWith(1, 2), addedRelationshipsWithType);
    }

    @Test
    void shouldGetRemovedRelationshipsByType() {
        // GIVEN
        state.relationshipDoDelete(1, 1, 1, 1);
        state.relationshipDoDelete(2, 1, 1, 1);
        state.relationshipDoDelete(3, 2, 1, 1);

        // WHEN
        LongSet removedRelationshipsWithType =
                state.relationshipsWithTypeChanged(1).getRemoved();

        // THEN
        assertEquals(newSetWith(1, 2), removedRelationshipsWithType);
    }

    @Test
    void removeAddedRelationshipTypeShouldRemoveFromAdded() {
        // GIVEN
        state.relationshipDoCreate(1, 1, 1, 1);
        state.relationshipDoCreate(2, 1, 1, 1);
        state.relationshipDoCreate(3, 2, 1, 1);

        // WHEN
        state.relationshipDoDelete(2, 1, 1, 1);
        LongSet addedRelationshipsWithType =
                state.relationshipsWithTypeChanged(1).getAdded();

        // THEN
        assertEquals(newSetWith(1), addedRelationshipsWithType);
    }

    @Test
    void addRemovedRelationshipTypeShouldRemoveFromRemoved() {
        // GIVEN
        state.relationshipDoDelete(1, 1, 1, 1);
        state.relationshipDoDelete(2, 1, 1, 1);
        state.relationshipDoDelete(3, 2, 1, 1);

        // WHEN
        state.relationshipDoCreate(2, 1, 1, 1);
        LongSet removedRelationshipsWithType =
                state.relationshipsWithTypeChanged(1).getRemoved();

        // THEN
        assertEquals(newSetWith(1), removedRelationshipsWithType);
    }

    @Test
    void removeRelationshipAddedInThisTxShouldRemoveFromTypeChanges() {
        // GIVEN
        state.relationshipDoCreate(1, 1, 1, 1);
        state.relationshipDoCreate(2, 1, 1, 1);
        state.relationshipDoCreate(3, 2, 1, 1);

        // WHEN
        state.relationshipDoDeleteAddedInThisBatch(2);
        LongSet addedRelationshipsWithType =
                state.relationshipsWithTypeChanged(1).getAdded();

        // THEN
        assertEquals(newSetWith(1), addedRelationshipsWithType);
    }

    @Test
    void shouldComputeIndexUpdatesOnUninitializedTxState() {
        // WHEN
        UnmodifiableMap<ValueTuple, ? extends LongDiffSets> diffSets = state.getIndexUpdates(indexOn_1_1.schema());

        // THEN
        assertNull(diffSets);
    }

    @Test
    void shouldComputeSortedIndexUpdatesOnUninitializedTxState() {
        // WHEN
        NavigableMap<ValueTuple, ? extends LongDiffSets> diffSets = state.getSortedIndexUpdates(indexOn_1_1.schema());

        // THEN
        assertNull(diffSets);
    }

    @Test
    void shouldComputeIndexUpdatesOnEmptyTxState() {
        // GIVEN
        addNodesToIndex(indexOn_2_1).withDefaultStringProperties(42L);

        // WHEN
        UnmodifiableMap<ValueTuple, ? extends LongDiffSets> diffSets = state.getIndexUpdates(indexOn_1_1.schema());

        // THEN
        assertNull(diffSets);
    }

    @Test
    void shouldComputeSortedIndexUpdatesOnEmptyTxState() {
        // GIVEN
        addNodesToIndex(indexOn_2_1).withDefaultStringProperties(42L);

        // WHEN
        NavigableMap<ValueTuple, ? extends LongDiffSets> diffSets = state.getSortedIndexUpdates(indexOn_1_1.schema());

        // THEN
        assertNull(diffSets);
    }

    @Test
    void shouldComputeIndexUpdatesOnTxStateWithAddedNodes() {
        // GIVEN
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(42L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(43L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(41L);

        // WHEN
        UnmodifiableMap<ValueTuple, ? extends LongDiffSets> diffSets = state.getIndexUpdates(indexOn_1_1.schema());

        // THEN
        assertNotNull(diffSets);
        assertEqualDiffSets(addedNodes(42L), diffSets.get(ValueTuple.of(stringValue("value42"))));
        assertEqualDiffSets(addedNodes(43L), diffSets.get(ValueTuple.of(stringValue("value43"))));
        assertEqualDiffSets(addedNodes(41L), diffSets.get(ValueTuple.of(stringValue("value41"))));
    }

    @Test
    void shouldComputeSortedIndexUpdatesOnTxStateWithAddedNodes() {
        // GIVEN
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(42L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(43L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(41L);

        // WHEN
        NavigableMap<ValueTuple, ? extends LongDiffSets> diffSets = state.getSortedIndexUpdates(indexOn_1_1.schema());

        TreeMap<ValueTuple, LongDiffSets> expected = sortedAddedNodesDiffSets(42, 41, 43);
        // THEN
        assertNotNull(diffSets);
        assertEquals(expected.keySet(), diffSets.keySet());
        for (final ValueTuple key : expected.keySet()) {
            assertEqualDiffSets(expected.get(key), diffSets.get(key));
        }
    }

    @Test
    void shouldAddAndGetByLabel() {
        // WHEN
        state.indexDoAdd(indexOn_1_1);
        state.indexDoAdd(indexOn_2_1);

        // THEN
        SchemaDescriptor schema = indexOn_1_1.schema();
        int[] labels = schema.getEntityTokenIds();
        assertEquals(schema.entityType(), EntityType.NODE);
        assertEquals(1, labels.length);
        assertEquals(asSet(indexOn_1_1), state.indexDiffSetsByLabel(labels[0]).getAdded());
    }

    @Test
    void shouldAddAndGetByRelType() {
        // WHEN
        state.indexDoAdd(indexOnRels);
        state.indexDoAdd(indexOn_2_1);

        // THEN
        assertEquals(
                asSet(indexOnRels),
                state.indexDiffSetsByRelationshipType(indexOnRels.schema().getRelTypeId())
                        .getAdded());
    }

    @Test
    void shouldAddAndGetByRuleId() {
        // GIVEN
        state.indexDoAdd(indexOn_1_1);

        // THEN
        assertEquals(asSet(indexOn_1_1), state.indexChanges().getAdded());
    }

    @Test
    void shouldListNodeAsDeletedIfItIsDeleted() {
        // Given

        // When
        long nodeId = 1337L;
        state.nodeDoDelete(nodeId);

        // Then
        assertThat(state.addedAndRemovedNodes().getRemoved()).isEqualTo(newSetWith(nodeId));
    }

    @Test
    void shouldAddUniquenessConstraint() {
        // when
        LabelSchemaDescriptor schema = forLabel(1, 17);
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForSchema(schema);
        state.constraintDoAdd(
                constraint,
                IndexPrototype.uniqueForSchema(schema).withName("constraint_7").materialise(7));

        // then
        DiffSets<ConstraintDescriptor> diff = state.constraintsChangesForLabel(1);

        assertEquals(singleton(constraint), diff.getAdded());
        assertTrue(diff.getRemoved().isEmpty());
    }

    @Test
    void addingUniquenessConstraintShouldBeIdempotent() {
        // given
        LabelSchemaDescriptor schema = forLabel(1, 17);
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForSchema(schema);
        state.constraintDoAdd(
                constraint1,
                IndexPrototype.uniqueForSchema(schema).withName("constraint_7").materialise(7));

        // when
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForSchema(schema);
        state.constraintDoAdd(
                constraint2,
                IndexPrototype.uniqueForSchema(schema).withName("constraint_19").materialise(19));

        // then
        assertEquals(constraint1, constraint2);
        assertEquals(singleton(constraint1), state.constraintsChangesForLabel(1).getAdded());
    }

    @Test
    void shouldDifferentiateBetweenUniquenessConstraintsForDifferentLabels() {
        // when
        LabelSchemaDescriptor schema1 = forLabel(1, 17);
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForSchema(schema1);
        state.constraintDoAdd(
                constraint1,
                IndexPrototype.uniqueForSchema(schema1).withName("constraint_7").materialise(7));
        LabelSchemaDescriptor schema2 = forLabel(2, 17);
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForSchema(schema2);
        state.constraintDoAdd(
                constraint2,
                IndexPrototype.uniqueForSchema(schema2)
                        .withName("constraint_19")
                        .materialise(19));

        // then
        assertEquals(singleton(constraint1), state.constraintsChangesForLabel(1).getAdded());
        assertEquals(singleton(constraint2), state.constraintsChangesForLabel(2).getAdded());
    }

    @Test
    void shouldAddRelationshipPropertyExistenceConstraint() {
        // Given
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType(1, 42);

        // When
        state.constraintDoAdd(constraint);

        // Then
        assertEquals(
                singleton(constraint),
                state.constraintsChangesForRelationshipType(1).getAdded());
    }

    @Test
    void addingRelationshipPropertyExistenceConstraintConstraintShouldBeIdempotent() {
        // Given
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.existsForRelType(1, 42);
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.existsForRelType(1, 42);

        // When
        state.constraintDoAdd(constraint1);
        state.constraintDoAdd(constraint2);

        // Then
        assertEquals(constraint1, constraint2);
        assertEquals(
                singleton(constraint1),
                state.constraintsChangesForRelationshipType(1).getAdded());
    }

    @Test
    void shouldDropRelationshipPropertyExistenceConstraint() {
        // Given
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType(1, 42);
        state.constraintDoAdd(constraint);

        // When
        state.constraintDoDrop(constraint);

        // Then
        assertTrue(state.constraintsChangesForRelationshipType(1).isEmpty());
    }

    @Test
    void shouldDifferentiateRelationshipPropertyExistenceConstraints() {
        // Given
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.existsForRelType(1, 11);
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.existsForRelType(1, 22);
        ConstraintDescriptor constraint3 = ConstraintDescriptorFactory.existsForRelType(3, 33);

        // When
        state.constraintDoAdd(constraint1);
        state.constraintDoAdd(constraint2);
        state.constraintDoAdd(constraint3);

        // Then
        assertEquals(
                asSet(constraint1, constraint2),
                state.constraintsChangesForRelationshipType(1).getAdded());
        assertEquals(
                singleton(constraint1),
                state.constraintsChangesForSchema(constraint1.schema()).getAdded());
        assertEquals(
                singleton(constraint2),
                state.constraintsChangesForSchema(constraint2.schema()).getAdded());
        assertEquals(
                singleton(constraint3),
                state.constraintsChangesForRelationshipType(3).getAdded());
        assertEquals(
                singleton(constraint3),
                state.constraintsChangesForSchema(constraint3.schema()).getAdded());
    }

    @Test
    void shouldListRelationshipsAsCreatedIfCreated() {
        // When
        long relId = 10;
        state.relationshipDoCreate(relId, 0, 1, 2);

        // Then
        assertTrue(state.hasChanges());
        assertTrue(state.relationshipIsAddedInThisBatch(relId));
    }

    @Test
    void shouldNotChangeRecordForCreatedAndDeletedNode() throws Exception {
        // GIVEN
        state.nodeDoCreate(0);
        state.nodeDoDelete(0);
        state.nodeDoCreate(1);

        // WHEN
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitCreatedNode(long id) {
                assertEquals(1, id, "Should not create any other node than 1");
            }

            @Override
            public void visitDeletedNode(long id) {
                fail("Should not delete any node");
            }
        });
    }

    @Test
    void shouldVisitDeletedNode() throws Exception {
        // Given
        state.nodeDoDelete(42);

        // When
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitDeletedNode(long id) {
                // Then
                assertEquals(42, id, "Wrong deleted node id");
            }
        });
    }

    @Test
    void shouldReportDeletedNodeIfItWasCreatedAndDeletedInSameTx() {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate(nodeId);
        state.nodeDoDelete(nodeId);

        // Then
        assertTrue(state.nodeIsDeletedInThisBatch(nodeId));
    }

    @Test
    void shouldNotReportDeletedNodeIfItIsNotDeleted() {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate(nodeId);

        // Then
        assertFalse(state.nodeIsDeletedInThisBatch(nodeId));
    }

    @ParameterizedTest
    @MethodSource("nodeModificationChanges")
    void testNodeIsModifiedInThisBatch(NodeStateModifier modifier, boolean reportAsModified) {
        final var nodeId = 42L;
        modifier.tweak(state, nodeId);
        assertThat(state.nodeIsModifiedInThisBatch(nodeId)).isEqualTo(reportAsModified);
    }

    @Test
    void shouldNotChangeRecordForCreatedAndDeletedRelationship() throws Exception {
        // GIVEN
        state.relationshipDoCreate(0, 0, 1, 2);
        state.relationshipDoDelete(0, 0, 1, 2);
        state.relationshipDoCreate(1, 0, 2, 3);

        // WHEN
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitRelationshipModifications(RelationshipModifications modifications) {
                modifications
                        .creations()
                        .forEach((id, type, startNode, endNode, addedProps) ->
                                assertEquals(1, id, "Should not create any other relationship than 1"));
                modifications
                        .deletions()
                        .forEach((id, type, startNode, endNode, noProps) -> fail("Should not delete any relationship"));
            }
        });
    }

    @Test
    void doNotVisitNotModifiedPropertiesOnModifiedNodes() throws KernelException {
        state.nodeDoAddLabel(5, 1);
        MutableBoolean labelsChecked = new MutableBoolean();
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitNodeLabelChanges(long id, LongSet added, LongSet removed) {
                labelsChecked.setTrue();
                assertEquals(1, id);
                assertEquals(1, added.size());
                assertTrue(added.contains(5));
                assertTrue(removed.isEmpty());
            }

            @Override
            public void visitNodePropertyChanges(
                    long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed) {
                fail("Properties were not changed.");
            }
        });
        assertTrue(labelsChecked.booleanValue());
    }

    @Test
    void doNotVisitNotModifiedLabelsOnModifiedNodes() throws KernelException {
        state.nodeDoAddProperty(1, 2, stringValue("propertyValue"));
        MutableBoolean propertiesChecked = new MutableBoolean();
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitNodeLabelChanges(long id, LongSet added, LongSet removed) {
                fail("Labels were not changed.");
            }

            @Override
            public void visitNodePropertyChanges(
                    long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed) {
                propertiesChecked.setTrue();
                assertEquals(1, id);
                assertFalse(changed.iterator().hasNext());
                assertTrue(removed.isEmpty());
                assertEquals(1, Iterators.count(added.iterator(), Predicates.alwaysTrue()));
            }
        });
        assertTrue(propertiesChecked.booleanValue());
    }

    @Test
    void shouldVisitDeletedRelationship() throws Exception {
        // Given
        state.relationshipDoDelete(42, 2, 3, 4);

        // When
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitRelationshipModifications(RelationshipModifications ids) {
                // Then
                assertThat(ids.deletions().size()).isEqualTo(1);
                ids.deletions()
                        .forEach((id, type, start, end, noProps) ->
                                assertEquals(42, id, "Wrong deleted relationship id"));
            }
        });
    }

    @Test
    void shouldReportDeletedRelationshipIfItWasCreatedAndDeletedInSameTx() {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate(relationshipId, relationshipType, startNodeId, endNodeId);
        state.relationshipDoDelete(relationshipId, relationshipType, startNodeId, endNodeId);

        // Then
        assertTrue(state.relationshipIsDeletedInThisBatch(relationshipId));
    }

    @Test
    void shouldNotReportDeletedRelationshipIfItIsNotDeleted() {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate(relationshipId, relationshipType, startNodeId, endNodeId);

        // Then
        assertFalse(state.relationshipIsDeletedInThisBatch(relationshipId));
    }

    @RepeatedTest(100)
    void shouldVisitCreatedNodesBeforeDeletedNodes() throws Exception {
        // when
        state.accept(new VisitationOrder(random.nextInt(100)) {
            // given

            @Override
            void createEarlyState() {
                state.nodeDoCreate(/*id=*/ random.nextInt(1 << 20));
            }

            @Override
            void createLateState() {
                state.nodeDoDelete(/*id=*/ random.nextInt(1 << 20));
            }

            // then

            @Override
            public void visitCreatedNode(long id) {
                visitEarly();
            }

            @Override
            public void visitDeletedNode(long id) {
                visitLate();
            }
        });
    }

    @RepeatedTest(100)
    void shouldVisitCreatedNodesBeforeCreatedRelationships() throws Exception {
        // when
        state.accept(new VisitationOrder(random.nextInt(100)) {
            // given

            @Override
            void createEarlyState() {
                state.nodeDoCreate(/*id=*/ random.nextInt(1 << 20));
            }

            @Override
            void createLateState() {
                state.relationshipDoCreate(
                        /*id=*/ random.nextInt(1 << 20),
                        /*type=*/ random.nextInt(128),
                        /*startNode=*/ random.nextInt(1 << 20),
                        /*endNode=*/ random.nextInt(1 << 20));
            }

            // then

            @Override
            public void visitCreatedNode(long id) {
                visitEarly();
            }

            @Override
            public void visitRelationshipModifications(RelationshipModifications modifications) {
                if (!modifications.creations().isEmpty()) {
                    visitLate();
                }
            }
        });
    }

    @RepeatedTest(100)
    void shouldVisitCreatedRelationshipsBeforeDeletedRelationships() throws Exception {
        // when
        state.accept(new VisitationOrder(random.nextInt(100)) {
            // given

            @Override
            void createEarlyState() {
                state.relationshipDoCreate(
                        /*id=*/ random.nextInt(1 << 20),
                        /*type=*/ random.nextInt(128),
                        /*startNode=*/ random.nextInt(1 << 20),
                        /*endNode=*/ random.nextInt(1 << 20));
            }

            @Override
            void createLateState() {
                state.relationshipDoDelete(
                        /*id=*/ random.nextInt(1 << 20),
                        /*type=*/ random.nextInt(128),
                        /*startNode=*/ random.nextInt(1 << 20),
                        /*endNode=*/ random.nextInt(1 << 20));
            }

            // then
            @Override
            public void visitRelationshipModifications(RelationshipModifications modifications) {
                if (!modifications.creations().isEmpty()) {
                    visitEarly();
                }
                if (!modifications.deletions().isEmpty()) {
                    visitLate();
                }
            }
        });
    }

    @RepeatedTest(100)
    void shouldVisitDeletedNodesAfterDeletedRelationships() throws Exception {
        // when
        state.accept(new VisitationOrder(random.nextInt(100)) {
            // given

            @Override
            void createEarlyState() {
                state.relationshipDoCreate(
                        /*id=*/ random.nextInt(1 << 20),
                        /*type=*/ random.nextInt(128),
                        /*startNode=*/ random.nextInt(1 << 20),
                        /*endNode=*/ random.nextInt(1 << 20));
            }

            @Override
            void createLateState() {
                state.nodeDoDelete(/*id=*/ random.nextInt(1 << 20));
            }

            // then

            @Override
            public void visitRelationshipModifications(RelationshipModifications ids) {
                visitEarly();
            }

            @Override
            public void visitDeletedNode(long id) {
                visitLate();
            }
        });
    }

    @Test
    void dataRevisionMustChangeOnDataChange() {
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());
        LongHashSet observedRevisions = new LongHashSet();
        observedRevisions.add(0L);

        state.nodeDoCreate(0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoAddLabel(0, 0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoRemoveLabel(0, 0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoAddProperty(0, 0, Values.booleanValue(true));
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoChangeProperty(0, 0, Values.booleanValue(false));
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoRemoveProperty(0, 0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoDelete(0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoCreate(0, 0, 0, 0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoReplaceProperty(0, 0, 0, 0, 0, Values.NO_VALUE, Values.booleanValue(true));
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoReplaceProperty(0, 0, 0, 0, 0, Values.booleanValue(true), Values.booleanValue(false));
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoRemoveProperty(0, 0, 0, 0, 0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoDeleteAddedInThisBatch(0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoDelete(1, 0, 0, 0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());
    }

    @Test
    void dataRevisionMustNotChangeOnSchemaChanges() {
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.indexDoAdd(indexOn_1_1);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.indexDoDrop(indexOn_1_1);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.indexDoUnRemove(indexOn_1_1);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel(1, 17);
        state.constraintDoAdd(constraint1);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.constraintDoDrop(constraint1);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.constraintDoUnRemove(constraint1);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        IndexBackedConstraintDescriptor constraint2 = ConstraintDescriptorFactory.nodeKeyForLabel(0, 0);
        state.constraintDoAdd(
                constraint2,
                IndexPrototype.uniqueForSchema(forLabel(0, 0)).withName("index").materialise(0));
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.labelDoCreateForName("Label", false, 0);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.relationshipTypeDoCreateForName("REL", false, 0);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.propertyKeyDoCreateForName("prop", false, 0);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        // This is not strictly a schema-change, but it is a "non-data" change in that these will not transform into
        // store updates.
        // Or schema updates for that matter. We only do these to speed up the transaction state filtering of schema
        // index query results.
        state.indexDoUpdateEntry(
                indexOn_1_1.schema(),
                0,
                ValueTuple.of(Values.booleanValue(true)),
                ValueTuple.of(Values.booleanValue(false)));
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());
    }

    //    getOrCreateLabelStateNodeDiffSets

    @Test
    void getOrCreateNodeState_props_useCollectionsFactory() {
        final NodeStateImpl nodeState = state.getOrCreateNodeState(1);
        long memoryBefore = usedMemory();

        nodeState.addProperty(2, stringValue("foo"));
        nodeState.removeProperty(3);
        nodeState.changeProperty(4, stringValue("bar"));

        verify(collectionsFactory, times(2)).newObjectMap(any());
        verify(collectionsFactory).newLongSet(any());
        assertThat(usedMemory()).isGreaterThan(memoryBefore);
        verifyNoMoreInteractions(collectionsFactory);
    }

    @Test
    void getOrCreateLabelStateNodeDiffSets_useCollectionsFactory() {
        final MutableLongDiffSets diffSets = state.getOrCreateLabelStateNodeDiffSets(1);
        long memoryBefore = usedMemory();

        diffSets.add(1);
        diffSets.remove(2);

        verify(collectionsFactory, times(2)).newLongSet(any());
        assertThat(usedMemory()).isGreaterThan(memoryBefore);
        verifyNoMoreInteractions(collectionsFactory);
    }

    @Test
    void getOrCreateTypeStateRelationshipDiffSets_useCollectionsFactory() {
        final MutableLongDiffSets diffSets = state.getOrCreateTypeStateRelationshipDiffSets(1);
        long memoryBefore = usedMemory();

        diffSets.add(1);
        diffSets.remove(2);

        verify(collectionsFactory, times(2)).newLongSet(any());
        assertThat(usedMemory()).isGreaterThan(memoryBefore);
        verifyNoMoreInteractions(collectionsFactory);
    }

    @Test
    void getOrCreateIndexUpdatesForSeek_useCollectionsFactory() {
        final MutableLongDiffSets diffSets =
                state.getOrCreateIndexUpdatesForSeek(new HashMap<>(), ValueTuple.of(stringValue("test")));
        long memoryBefore = usedMemory();

        diffSets.add(1);
        diffSets.remove(2);

        verify(collectionsFactory, times(2)).newLongSet(any());
        assertThat(usedMemory()).isGreaterThan(memoryBefore);
        verifyNoMoreInteractions(collectionsFactory);
    }

    @Test
    void visitedRelationshipChangesShouldBeSortedByNode() throws KernelException {
        // Given
        for (int i = 0; i < 100; i++) {
            state.relationshipDoCreate(
                    /*id=*/ random.nextInt(1 << 20),
                    /*type=*/ random.nextInt(128),
                    /*startNode=*/ random.nextInt(1 << 20),
                    /*endNode=*/ random.nextInt(1 << 20));
        }

        // When
        List<Long> visitedRelationshipIds = new ArrayList<>();
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitRelationshipModifications(RelationshipModifications modifications) {
                modifications.forEachSplit(
                        nodeRelationshipIds -> visitedRelationshipIds.add(nodeRelationshipIds.nodeId()));
            }
        });
        // Then
        assertThat(visitedRelationshipIds).isNotEmpty();
        assertThat(visitedRelationshipIds).isSorted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldKeepMetaDataForDeletedRelationshipsIfToldTo(boolean keepDeletedRelationshipMetaData)
            throws KernelException {
        // given
        TxState state = new TxState(
                collectionsFactory,
                memoryTracker,
                () -> keepDeletedRelationshipMetaData,
                ApplyEnrichmentStrategy.NO_ENRICHMENT,
                ChunkedTransactionSink.EMPTY,
                TransactionEvent.NULL);
        long id = 9;
        int type = 10;
        long startNode = 11;
        long endNode = 12;
        state.relationshipDoDelete(id, type, startNode, endNode);

        // when/then
        MutableBoolean found = new MutableBoolean();
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitRelationshipModifications(RelationshipModifications modifications) {
                modifications.deletions().forEach((relationshipId, typeId, startNodeId, endNodeId, addedProperties) -> {
                    assertThat(found.booleanValue()).isFalse();
                    found.setTrue();

                    if (keepDeletedRelationshipMetaData) {
                        assertThat(relationshipId).isEqualTo(id);
                        assertThat(typeId).isEqualTo(type);
                        assertThat(startNodeId).isEqualTo(startNode);
                        assertThat(endNodeId).isEqualTo(endNode);
                    } else {
                        assertThat(relationshipId).isEqualTo(id);
                        assertThat(typeId).isEqualTo(-1);
                        assertThat(startNodeId).isEqualTo(-1);
                        assertThat(endNodeId).isEqualTo(-1);
                    }
                });
            }
        });
        assertThat(found.booleanValue()).isTrue();
    }

    @Test
    void transactionStateResetReleasesNodeUsedMemory() {
        long memoryOnTestStart = usedMemory();

        for (int iteration = 0; iteration < 10; iteration++) {
            for (int nodeId = 0; nodeId < 1024; nodeId++) {
                state.nodeDoCreate(nodeId);
            }
            assertThat(usedMemory()).isGreaterThan(memoryOnTestStart);

            state.reset();

            assertEquals(usedMemory(), memoryOnTestStart);
        }
    }

    @Test
    void transactionStateResetReleasesRelatioshipUsedMemory() {
        long memoryOnTestStart = usedMemory();

        for (int iteration = 0; iteration < 10; iteration++) {
            for (int relationship = 0; relationship < 1024; relationship++) {
                state.relationshipDoDelete(relationship, 2, relationship + 1, relationship + 2);
            }
            assertThat(usedMemory()).isGreaterThan(memoryOnTestStart);

            state.reset();

            assertEquals(usedMemory(), memoryOnTestStart);
        }
    }

    @Test
    void stateResetKeepChangesFlagsOn() {
        state.nodeDoCreate(1);

        assertTrue(state.hasDataChanges());
        assertTrue(state.hasChanges());

        state.reset();

        assertTrue(state.hasDataChanges());
        assertTrue(state.hasChanges());
    }

    private LongDiffSets addedNodes(long... added) {
        return new MutableLongDiffSetsImpl(
                LongSets.mutable.of(added), LongSets.mutable.empty(), collectionsFactory, memoryTracker);
    }

    private TreeMap<ValueTuple, LongDiffSets> sortedAddedNodesDiffSets(long... added) {
        TreeMap<ValueTuple, LongDiffSets> map = new TreeMap<>(ValueTuple.COMPARATOR);
        for (long node : added) {

            map.put(ValueTuple.of(stringValue("value" + node)), addedNodes(node));
        }
        return map;
    }

    abstract class VisitationOrder extends TxStateVisitor.Adapter {
        private final Set<String> visitMethods = new HashSet<>();

        VisitationOrder(int size) {
            for (Method method : getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("visit")) {
                    visitMethods.add(method.getName());
                }
            }
            do {
                if (random.nextBoolean()) {
                    createEarlyState();
                } else {
                    createLateState();
                }
            } while (size-- > 0);
        }

        abstract void createEarlyState();

        abstract void createLateState();

        private boolean late;

        final void visitEarly() {
            if (late) {
                String early = "the early visit*-method";
                String late = "the late visit*-method";
                for (StackTraceElement trace : Thread.currentThread().getStackTrace()) {
                    if (visitMethods.contains(trace.getMethodName())) {
                        early = trace.getMethodName();
                        for (String method : visitMethods) {
                            if (!method.equals(early)) {
                                late = method;
                            }
                        }
                        break;
                    }
                }
                fail(early + "(...) should not be invoked after " + late + "(...)");
            }
        }

        final void visitLate() {
            late = true;
        }
    }

    private interface IndexUpdater {
        void withDefaultStringProperties(long... nodeIds);
    }

    private IndexUpdater addNodesToIndex(final IndexDescriptor descriptor) {
        return new IndexUpdater() {
            @Override
            public void withDefaultStringProperties(long... nodeIds) {
                Collection<Pair<Long, String>> entries = new ArrayList<>(nodeIds.length);
                for (long nodeId : nodeIds) {
                    entries.add(of(nodeId, "value" + nodeId));
                }
                withProperties(entries);
            }

            private <T> void withProperties(Collection<Pair<Long, T>> nodesWithValues) {
                SchemaDescriptor schema = descriptor.schema();
                int[] labelIds = schema.getEntityTokenIds();
                int[] propertyKeyIds = schema.getPropertyIds();
                assertEquals(1, labelIds.length);
                assertEquals(1, propertyKeyIds.length);
                for (Pair<Long, T> entry : nodesWithValues) {
                    long nodeId = entry.first();
                    state.nodeDoCreate(nodeId);
                    state.nodeDoAddLabel(labelIds[0], nodeId);
                    Value valueAfter = Values.of(entry.other());
                    state.nodeDoAddProperty(nodeId, propertyKeyIds[0], valueAfter);
                    state.indexDoUpdateEntry(schema, nodeId, null, ValueTuple.of(valueAfter));
                }
            }
        };
    }

    private static void assertEqualDiffSets(LongDiffSets expected, LongDiffSets actual) {
        assertEquals(expected.getRemoved(), actual.getRemoved());
        assertEquals(expected.getAdded(), actual.getAdded());
    }

    @FunctionalInterface
    private interface NodeStateModifier {
        void tweak(TxState state, long nodeId);
    }

    private static Stream<Arguments> nodeModificationChanges() {
        return Stream.of(
                Arguments.of(
                        (NodeStateModifier)
                                (state, nodeId) -> state.nodeDoAddProperty(nodeId, 42, Values.stringValue("changed")),
                        true),
                Arguments.of(
                        (NodeStateModifier) (state, nodeId) ->
                                state.nodeDoChangeProperty(nodeId, 42, Values.stringValue("changed")),
                        true),
                Arguments.of((NodeStateModifier) (state, nodeId) -> state.nodeDoRemoveProperty(nodeId, 42), true),
                Arguments.of((NodeStateModifier) (state, nodeId) -> state.nodeDoAddLabel(42, nodeId), true),
                Arguments.of((NodeStateModifier) (state, nodeId) -> state.nodeDoRemoveLabel(42, nodeId), true),
                Arguments.of(
                        (NodeStateModifier) (state, nodeId) -> {
                            state.nodeDoCreate(nodeId);
                            state.nodeDoAddProperty(nodeId, 42, Values.stringValue("changed"));
                        },
                        false),
                Arguments.of(
                        (NodeStateModifier) (state, nodeId) -> {
                            state.nodeDoChangeProperty(nodeId, 42, Values.stringValue("changed"));
                            state.nodeDoDelete(nodeId);
                        },
                        false));
    }
}
