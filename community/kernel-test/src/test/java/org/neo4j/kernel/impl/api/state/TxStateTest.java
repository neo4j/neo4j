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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.UnmodifiableMap;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.collection.diffset.DiffSets;
import org.neo4j.collection.diffset.MutableLongDiffSets;
import org.neo4j.collection.factory.CollectionsFactory;
import org.neo4j.collection.factory.OnHeapCollectionsFactory;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationshipsException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexRemovalSnapshot;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransactionSink;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipVisitorWithProperties;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.TransactionStateBehaviour;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.storageengine.api.txstate.memory.TxStateMemoryConsumer;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
class TxStateTest {
    @Inject
    private RandomSupport random;

    private final IndexDescriptor indexOn_1_1 = TestIndexDescriptorFactory.forLabel(1, 1);
    private final IndexDescriptor indexOn_2_1 = TestIndexDescriptorFactory.forLabel(2, 1);
    private final IndexDescriptor indexOnRels =
            TestIndexDescriptorFactory.forSchema(SchemaDescriptors.forRelType(3, 1));
    private CollectionsFactory collectionsFactory;
    private TxState state;
    MemoryTracker memoryTracker;

    @BeforeEach
    void before() {
        memoryTracker = new LocalMemoryTracker();
        collectionsFactory = spy(OnHeapCollectionsFactory.INSTANCE);
        state = new TxState(
                collectionsFactory,
                memoryTracker,
                TransactionStateBehaviour.DEFAULT_BEHAVIOUR,
                ApplyEnrichmentStrategy.NO_ENRICHMENT,
                ChunkedTransactionSink.EMPTY,
                TxStateMemoryConsumer.EMPTY_CONSUMER,
                TransactionEvent.NULL);
    }

    @AfterEach
    void after() {
        collectionsFactory.release();
        assertEquals(0L, memoryTracker.usedNativeMemory(), "Seems like native memory is leaking");
    }

    long usedMemory() {
        return memoryTracker.estimatedHeapMemory();
    }

    @Test
    void shouldGetAddedLabels() {
        // GIVEN
        state.nodeDoAddLabel(1, 0);
        state.nodeDoAddLabel(1, 1);
        state.nodeDoAddLabel(2, 1);

        // WHEN
        IntSet addedLabels = state.nodeStateLabelDiffSets(1).getAdded();

        // THEN
        assertEquals(IntHashSet.newSetWith(1, 2), addedLabels);
    }

    @Test
    void shouldGetRemovedLabels() {
        // GIVEN
        state.nodeDoRemoveLabel(1, 0);
        state.nodeDoRemoveLabel(1, 1);
        state.nodeDoRemoveLabel(2, 1);

        // WHEN
        IntSet removedLabels = state.nodeStateLabelDiffSets(1).getRemoved();

        // THEN
        assertEquals(IntHashSet.newSetWith(1, 2), removedLabels);
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
        assertEquals(IntHashSet.newSetWith(2), state.nodeStateLabelDiffSets(1).getAdded());
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
        assertEquals(IntHashSet.newSetWith(2), state.nodeStateLabelDiffSets(1).getRemoved());
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
        UnmodifiableMap<ValueTuple, MutableLongSet> added = state.getAddedIndexUpdates(indexOn_1_1);

        // THEN
        assertNull(added);
    }

    @Test
    void shouldComputeSortedIndexUpdatesOnUninitializedTxState() {
        // WHEN
        NavigableMap<ValueTuple, MutableLongSet> added = state.getSortedAddedIndexUpdates(indexOn_1_1);

        // THEN
        assertNull(added);
    }

    @Test
    void shouldComputeIndexRemovedIdsOnUninitializedTxState() {
        // WHEN
        IndexRemovalSnapshot removed = state.getRemovedFromIndex(indexOn_1_1);

        // THEN
        assertNull(removed);
    }

    @Test
    void shouldComputeIndexUpdatesOnEmptyTxState() {
        // GIVEN
        addNodesToIndex(indexOn_2_1).withDefaultStringProperties(42L);

        // WHEN
        UnmodifiableMap<ValueTuple, MutableLongSet> added = state.getAddedIndexUpdates(indexOn_1_1);

        // THEN
        assertNull(added);
    }

    @Test
    void shouldComputeSortedIndexUpdatesOnEmptyTxState() {
        // GIVEN
        addNodesToIndex(indexOn_2_1).withDefaultStringProperties(42L);

        // WHEN
        NavigableMap<ValueTuple, MutableLongSet> added = state.getSortedAddedIndexUpdates(indexOn_1_1);

        // THEN
        assertNull(added);
    }

    @Test
    void shouldComputeIndexRemovedIdsOnEmptyTxState() {
        // GIVEN
        addNodesToIndex(indexOn_2_1).withDefaultStringProperties(42L);

        // WHEN
        IndexRemovalSnapshot removed = state.getRemovedFromIndex(indexOn_1_1);

        // THEN
        assertNull(removed);
    }

    @Test
    void shouldComputeIndexUpdatesOnTxStateWithAddedNodes() {
        // GIVEN
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(42L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(43L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(41L);

        // WHEN
        UnmodifiableMap<ValueTuple, MutableLongSet> added = state.getAddedIndexUpdates(indexOn_1_1);
        IndexRemovalSnapshot removedIds = state.getRemovedFromIndex(indexOn_1_1);

        // THEN
        assertNotNull(added);
        assertThat(added.size()).isEqualTo(3);
        assertThat(added.keySet())
                .containsExactlyInAnyOrder(
                        ValueTuple.of(stringValue("value41")),
                        ValueTuple.of(stringValue("value43")),
                        ValueTuple.of(stringValue("value42")));
        assertThat(added.get(ValueTuple.of(stringValue("value41"))).toArray()).containsExactly(41L);
        assertThat(added.get(ValueTuple.of(stringValue("value42"))).toArray()).containsExactly(42L);
        assertThat(added.get(ValueTuple.of(stringValue("value43"))).toArray()).containsExactly(43L);

        assertNotNull(removedIds);
        assertTrue(removedIds.isEmpty());
    }

    @Test
    void shouldComputeSortedIndexUpdatesOnTxStateWithAddedNodes() {
        // GIVEN
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(42L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(43L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(41L);

        // WHEN
        NavigableMap<ValueTuple, MutableLongSet> added = state.getSortedAddedIndexUpdates(indexOn_1_1);
        // THEN
        assertNotNull(added);
        assertThat(added.keySet())
                .containsExactly(
                        ValueTuple.of(stringValue("value41")),
                        ValueTuple.of(stringValue("value42")),
                        ValueTuple.of(stringValue("value43")));
        assertThat(added.get(ValueTuple.of(stringValue("value41"))).toArray()).containsExactly(41L);
        assertThat(added.get(ValueTuple.of(stringValue("value42"))).toArray()).containsExactly(42L);
        assertThat(added.get(ValueTuple.of(stringValue("value43"))).toArray()).containsExactly(43L);
    }

    @Test
    void shouldComputeIndexUpdatesOnTxStateWithAddedAndRemovedNodes() {
        // GIVEN
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(42L);
        addNodesToIndex(indexOn_1_1).withDefaultStringProperties(43L);

        state.indexDoUpdateEntry(indexOn_1_1, 40L, ValueTuple.of(stringValue("value40")), null);
        state.indexDoUpdateEntry(indexOn_1_1, 41L, ValueTuple.of(stringValue("value41")), null);

        // WHEN
        UnmodifiableMap<ValueTuple, MutableLongSet> added = state.getAddedIndexUpdates(indexOn_1_1);
        IndexRemovalSnapshot removedIds = state.getRemovedFromIndex(indexOn_1_1);

        // THEN
        assertNotNull(added);
        assertThat(added.size()).isEqualTo(2);
        assertThat(added.keySet())
                .containsExactlyInAnyOrder(
                        ValueTuple.of(stringValue("value43")), ValueTuple.of(stringValue("value42")));
        assertThat(added.get(ValueTuple.of(stringValue("value42"))).toArray()).containsExactly(42L);
        assertThat(added.get(ValueTuple.of(stringValue("value43"))).toArray()).containsExactly(43L);

        assertNotNull(removedIds);
        assertTrue(removedIds.isRemoved().test(40L));
        assertTrue(removedIds.isRemoved().test(41L));
    }

    @Test
    void shouldUpdateRemovalVersionOfIndex() {
        // WHEN
        state.indexDoUpdateEntry(indexOn_1_1, 40L, ValueTuple.of(stringValue("value40")), null);
        IndexRemovalSnapshot r1 = state.getRemovedFromIndex(indexOn_1_1);
        state.indexDoUpdateEntry(indexOn_1_1, 41L, ValueTuple.of(stringValue("value41")), null);
        IndexRemovalSnapshot r2 = state.getRemovedFromIndex(indexOn_1_1);

        // THEN
        assertNotNull(r1);
        assertNotNull(r2);
        assertFalse(r1.isEmpty());
        assertTrue(r1.isRemoved().test(40L));
        assertFalse(r1.isRemoved().test(41L));
        assertThat(r1.version()).isEqualTo(0);

        assertFalse(r2.isEmpty());
        assertTrue(r2.isRemoved().test(40L));
        assertTrue(r2.isRemoved().test(41L));
        assertThat(r2.version()).isEqualTo(1);
    }

    @Test
    void shouldUpdateIndexRemovalForEachIndexSeparately() {
        // WHEN
        state.indexDoUpdateEntry(indexOn_1_1, 40L, ValueTuple.of(stringValue("value40")), null);
        IndexRemovalSnapshot r1 = state.getRemovedFromIndex(indexOn_1_1);
        state.indexDoUpdateEntry(indexOn_2_1, 39L, ValueTuple.of(stringValue("value40")), null);
        IndexRemovalSnapshot rIndex2 = state.getRemovedFromIndex(indexOn_2_1);
        state.indexDoUpdateEntry(indexOn_1_1, 41L, ValueTuple.of(stringValue("value41")), null);
        IndexRemovalSnapshot r2 = state.getRemovedFromIndex(indexOn_1_1);

        // THEN
        assertNotNull(r1);
        assertFalse(r1.isEmpty());
        assertFalse(r1.isRemoved().test(39L));
        assertTrue(r1.isRemoved().test(40L));
        assertFalse(r1.isRemoved().test(41L));
        assertThat(r1.version()).isEqualTo(0);

        assertNotNull(r2);
        assertFalse(r2.isEmpty());
        assertTrue(r2.isRemoved().test(40L));
        assertTrue(r2.isRemoved().test(41L));
        assertThat(r2.version()).isEqualTo(1);

        assertNotNull(rIndex2);
        assertFalse(rIndex2.isEmpty());
        assertTrue(rIndex2.isRemoved().test(39L));
        assertFalse(rIndex2.isRemoved().test(40L));
        assertFalse(rIndex2.isRemoved().test(41L));
        assertThat(rIndex2.version()).isEqualTo(0);
    }

    @Test
    void shouldAddAndGetByLabel() {
        // WHEN
        state.indexDoAdd(indexOn_1_1);
        state.indexDoAdd(indexOn_2_1);

        // THEN
        SchemaDescriptor schema = indexOn_1_1.schema();
        int[] labels = schema.getEntityTokenIds();
        assertEquals(EntityType.NODE, schema.entityType());
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
        ConstraintDescriptor constraint =
                ConstraintDescriptorFactory.existsForRelType(false, 1, 42).withId(123);

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
        ConstraintDescriptor constraint1 =
                ConstraintDescriptorFactory.existsForRelType(false, 1, 42).withId(99);
        ConstraintDescriptor constraint2 =
                ConstraintDescriptorFactory.existsForRelType(false, 1, 42).withId(999);

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
        ConstraintDescriptor constraint =
                ConstraintDescriptorFactory.existsForRelType(false, 1, 42).withId(11);
        state.constraintDoAdd(constraint);

        // When
        state.constraintDoDrop(constraint);

        // Then
        assertTrue(state.constraintsChangesForRelationshipType(1).isEmpty());
    }

    @Test
    void shouldDifferentiateRelationshipPropertyExistenceConstraints() {
        // Given
        ConstraintDescriptor constraint1 =
                ConstraintDescriptorFactory.existsForRelType(false, 1, 11).withId(1);
        ConstraintDescriptor constraint2 =
                ConstraintDescriptorFactory.existsForRelType(false, 1, 22).withId(2);
        ConstraintDescriptor constraint3 =
                ConstraintDescriptorFactory.existsForRelType(false, 3, 33).withId(3);

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

    @ParameterizedTest
    @MethodSource("relationshipIsModifiedTests")
    void testRelationshipIsModifiedInThisBatch(RelationshipIsModifiedTest test) {
        var txState = new TxState();
        test.txStateConsumer.accept(txState);
        assertThat(txState.relationshipsIsModifiedInThisBatch(RelationshipIsModifiedTest.REL_ID))
                .isEqualTo(test.expected());
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
                        .forEach((id, type, startNode, endNode, addedProps, removedProperties) ->
                                assertEquals(1, id, "Should not create any other relationship than 1"));
                modifications
                        .deletions()
                        .forEach((id, type, startNode, endNode, noProps, removedProperties) ->
                                fail("Should not delete any relationship"));
            }
        });
    }

    @Test
    void doNotVisitNotModifiedPropertiesOnModifiedNodes() throws KernelException {
        state.nodeDoAddLabel(5, 1);
        MutableBoolean labelsChecked = new MutableBoolean();
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitNodeLabelChanges(long id, IntSet added, IntSet removed) {
                labelsChecked.setTrue();
                assertEquals(1, id);
                assertEquals(1, added.size());
                assertTrue(added.contains(5));
                assertTrue(removed.isEmpty());
            }

            @Override
            public void visitNodePropertyChanges(long id, Iterable<StorageProperty> added, IntIterable removed) {
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
            public void visitNodeLabelChanges(long id, IntSet added, IntSet removed) {
                fail("Labels were not changed.");
            }

            @Override
            public void visitNodePropertyChanges(long id, Iterable<StorageProperty> added, IntIterable removed) {
                propertiesChecked.setTrue();
                assertEquals(1, id);
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
                        .forEach((id, type, start, end, noProps, removedProperties) ->
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
                state.relationshipDoDelete(
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

        state.nodeDoAddProperty(0, 0, Values.booleanValue(false));
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoRemoveProperty(0, 0, Predicates.ALWAYS_TRUE_INT);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.nodeDoDelete(0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoCreate(0, 0, 0, 0);
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoAddProperty(0, 0, 0, 0, 0, Values.booleanValue(true));
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoAddProperty(0, 0, 0, 0, 0, Values.booleanValue(false));
        assertTrue(observedRevisions.add(state.getDataRevision()));
        assertTrue(state.hasDataChanges());

        state.relationshipDoRemoveProperty(0, 0, 0, 0, 0, Predicates.ALWAYS_TRUE_INT);
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

        UniquenessConstraintDescriptor constraint1 =
                ConstraintDescriptorFactory.uniqueForLabel(1, 17).withId(12);
        state.constraintDoAdd(constraint1);
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());

        state.constraintDoDrop(constraint1);
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
                indexOn_1_1, 0, ValueTuple.of(Values.booleanValue(true)), ValueTuple.of(Values.booleanValue(false)));
        assertThat(state.getDataRevision()).isEqualTo(0L);
        assertFalse(state.hasDataChanges());
    }

    //    getOrCreateLabelStateNodeDiffSets

    @Test
    void getOrCreateNodeState_props_useCollectionsFactory() {
        final NodeStateImpl nodeState = state.getOrCreateNodeState(1);
        long memoryBefore = usedMemory();

        nodeState.addProperty(2, stringValue("foo"));
        nodeState.removePropertyFromStore(3);
        nodeState.addProperty(4, stringValue("bar"));

        verify(collectionsFactory, times(1)).newObjectMap(any());
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
    void creatingIndexUpdateShouldBeMemoryTacked() {
        long memoryBefore = usedMemory();
        state.indexDoUpdateEntry(indexOn_1_1, 0, ValueTuple.of(Values.intValue(1)), ValueTuple.of(Values.intValue(2)));
        assertThat(usedMemory()).isGreaterThan(memoryBefore);
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
                new TransactionStateBehaviour() {
                    @Override
                    public boolean keepMetaDataForDeletedRelationship() {
                        return keepDeletedRelationshipMetaData;
                    }

                    @Override
                    public boolean useIndexCommands() {
                        return false;
                    }
                },
                ApplyEnrichmentStrategy.NO_ENRICHMENT,
                ChunkedTransactionSink.EMPTY,
                TxStateMemoryConsumer.EMPTY_CONSUMER,
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
                modifications.deletions().forEach((rId, typeId, start, end, aP, rP) -> {
                    assertThat(found.booleanValue()).isFalse();
                    found.setTrue();

                    if (keepDeletedRelationshipMetaData) {
                        assertThat(rId).isEqualTo(id);
                        assertThat(typeId).isEqualTo(type);
                        assertThat(start).isEqualTo(startNode);
                        assertThat(end).isEqualTo(endNode);
                    } else {
                        assertThat(rId).isEqualTo(id);
                        assertThat(typeId).isEqualTo(-1);
                        assertThat(start).isEqualTo(-1);
                        assertThat(end).isEqualTo(-1);
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

    @Test
    void shouldThrowOnCreatedDeletedNodeWithRelationshipsLeft() {
        // GIVEN
        state.nodeDoCreate(0);
        state.nodeDoCreate(1);
        state.relationshipDoCreate(0, 0, 0, 1);
        state.nodeDoDelete(0);

        // WHEN
        assertThatThrownBy(() -> state.accept(new TxStateVisitor.Adapter() {}))
                .isInstanceOf(DeletedNodeStillHasRelationshipsException.class);
    }

    @Test
    void shouldThrowOnDeletedNodeWithRelationshipsAddedInTx() {
        // GIVEN
        state.nodeDoCreate(1);
        state.relationshipDoCreate(0, 0, 0, 1);
        state.nodeDoDelete(0);

        // WHEN
        assertThatThrownBy(() -> state.accept(new TxStateVisitor.Adapter() {}))
                .isInstanceOf(DeletedNodeStillHasRelationshipsException.class);
    }

    @Test
    void shouldGetChangedRelationshipPropertiesOnExistingRel() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();
        state.addProp(1, 1);
        state.addProp(2, 1);

        assertThat(state.hasStateChanges()).isTrue();
        assertRelModificationsMatch(state);
    }

    @Test
    void shouldGetChangedRelationshipPropertiesOnNewRel() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();
        state.create(1);
        state.addProp(1, 1);
        state.addProp(2, 1);

        assertThat(state.hasStateChanges()).isTrue();
        assertRelModificationsMatch(state);
    }

    @Test
    void shouldGetRemovedPropertiesOnExistingRel() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();
        state.removeProp(1, 1);

        assertThat(state.hasStateChanges()).isTrue();
        assertRelModificationsMatch(state);
    }

    @Test
    void shouldGetRemovedPropertiesOnNewRel() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();
        state.create(1);
        state.removeProp(1, 1);

        assertThat(state.hasStateChanges()).isTrue();
        assertRelModificationsMatch(state);
    }

    @Test
    void shouldNotGetAddedThenRemovedRelationshipProperty() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();
        state.addProp(1, 1);
        state.removeProp(1, 1);

        assertThat(state.hasStateChanges()).isFalse();
        assertRelModificationsMatch(state);
    }

    @Test
    void shouldNotGetAddedRelationshipPropertiesOnDeletedRel() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();
        state.addProp(1, 1);
        state.delete(1);

        assertThat(state.created).isEmpty();
        assertThat(state.updated).isEmpty();
        assertThat(state.deleted).isNotEmpty();
        assertRelModificationsMatch(state);
    }

    @Test
    void shouldNotGetAnythingOnAddedThenDeletedRelationship() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();
        state.create(1);
        state.addProp(1, 1);
        state.delete(1);

        assertThat(state.hasStateChanges()).isFalse();
        assertRelModificationsMatch(state);
    }

    @Test
    void shouldDealWithRandomRelUpdates() throws Exception {
        RelTxStateMirror state = new RelTxStateMirror();

        for (long id = 0; id < 100; id++) {
            if (random.nextInt(4) == 0) {
                state.create(id);
            }
            for (int key = 0; key < 10; key++) {
                switch (random.nextInt(3)) {
                    case 0 -> state.addProp(id, key);
                    case 1 -> state.addProp(id, key);
                    case 2 -> state.removeProp(id, key);
                }
            }

            if (random.nextInt(4) == 0) {
                state.delete(id);
            }
        }
        assertRelModificationsMatch(state);
    }

    private class RelTxStateMirror {
        private final Map<Long, RelData> created = new HashMap<>();
        private final Map<Long, RelData> deleted = new HashMap<>();
        private final Map<Long, RelData> updated = new HashMap<>();

        void create(long id) {
            assertThat(created).doesNotContainKey(id);
            assertThat(updated).doesNotContainKey(id);
            assertThat(deleted).doesNotContainKey(id);
            RelData data = new RelData(id, random);
            created.put(id, data);
            state.relationshipDoCreate(data.id, data.type, data.startNode, data.endNode);
        }

        void addProp(long id, int key) {
            setProp(id, key);
        }

        private void setProp(long id, int key) {
            assertThat(deleted).doesNotContainKey(id);

            RelData data = created.get(id);
            if (data == null) {
                data = updated.get(id);
                if (data == null) {
                    data = new RelData(id, random);
                    updated.put(id, data);
                }
            }
            PropertyKeyValue prop = new PropertyKeyValue(key, Values.intValue(1));
            data.addedProperties.add(prop);

            state.relationshipDoAddProperty(
                    data.id, data.type, data.startNode, data.endNode, prop.propertyKeyId(), prop.value());
        }

        void removeProp(long id, int key) {
            assertThat(deleted).doesNotContainKey(id);

            RelData data = created.get(id);
            if (data == null) {
                data = updated.get(id);
            }
            if (data == null) {
                data = new RelData(id, random);
                updated.put(id, data);
            }
            boolean removed = data.addedProperties.removeIf(storageProperty -> storageProperty.propertyKeyId() == key);
            if (!removed) {
                data.removedProperties.add(key);
            } else {
                if (data.addedProperties.isEmpty() && data.removedProperties.isEmpty()) {
                    updated.remove(id);
                }
            }
            state.relationshipDoRemoveProperty(data.id, data.type, data.startNode, data.endNode, key, k -> !removed);
        }

        void delete(long id) {
            RelData data;
            if (created.containsKey(id)) {
                assertThat(updated.containsKey(id)).isFalse();
                data = created.remove(id);
            } else {
                data = updated.remove(id);
                // Deletions are visited without data, only ID
                deleted.put(id, new RelData(id, -1, -1, -1, new HashSet<>(), IntSets.mutable.empty()));
            }

            state.relationshipDoDelete(data.id, data.type, data.startNode, data.endNode);
        }

        boolean hasStateChanges() {
            return !created.isEmpty() || !deleted.isEmpty() || !updated.isEmpty();
        }
    }

    private void assertRelModificationsMatch(RelTxStateMirror txStateMirror) throws KernelException {
        assertRelModificationsMatch(
                new HashSet<>(txStateMirror.created.values()),
                new HashSet<>(txStateMirror.updated.values()),
                new HashSet<>(txStateMirror.deleted.values()));
    }

    private void assertRelModificationsMatch(
            Set<RelData> createdExpected, Set<RelData> updatedExpected, Set<RelData> deletedExpected)
            throws KernelException {
        Set<RelData> created = new HashSet<>();
        Set<RelData> deleted = new HashSet<>();
        Set<RelData> updated = new HashSet<>();
        Set<RelData> createdSplitIn = new HashSet<>();
        Set<RelData> createdSplitOut = new HashSet<>();
        Set<RelData> deletedSplitIn = new HashSet<>();
        Set<RelData> deletedSplitOut = new HashSet<>();
        Set<RelData> updatedSplitIn = new HashSet<>();
        Set<RelData> updatedSplitOut = new HashSet<>();

        // WHEN
        state.accept(new TxStateVisitor.Adapter() {
            @Override
            public void visitRelationshipModifications(RelationshipModifications modifications) {
                modifications.updates().forEach(collector(updated));
                modifications.creations().forEach(collector(created));
                modifications.deletions().forEach(collector(deleted));

                modifications.forEachSplit(node -> {
                    node.forEachUpdateSplit(type -> {
                        type.in().forEach(collector(updatedSplitIn));
                        type.out().forEach(collector(updatedSplitOut));
                        type.loop().forEach(collector(updatedSplitIn));
                        type.loop().forEach(collector(updatedSplitOut));
                    });
                    node.forEachCreationSplit(type -> {
                        type.in().forEach(collector(createdSplitIn));
                        type.out().forEach(collector(createdSplitOut));
                        type.loop().forEach(collector(createdSplitIn));
                        type.loop().forEach(collector(createdSplitOut));
                    });
                    node.forEachDeletionSplit(type -> {
                        type.in().forEach(collector(deletedSplitIn));
                        type.out().forEach(collector(deletedSplitOut));
                        type.loop().forEach(collector(deletedSplitIn));
                        type.loop().forEach(collector(deletedSplitOut));
                    });
                });
            }
        });

        assertThat(created).isEqualTo(createdSplitIn).isEqualTo(createdSplitOut).isEqualTo(createdExpected);
        assertThat(deleted).isEqualTo(deletedSplitIn).isEqualTo(deletedSplitOut).isEqualTo(deletedExpected);
        assertThat(updated).isEqualTo(updatedSplitIn).isEqualTo(updatedSplitOut).isEqualTo(updatedExpected);
    }

    record RelData(
            long id,
            int type,
            long startNode,
            long endNode,
            Set<StorageProperty> addedProperties,
            MutableIntSet removedProperties) {
        RelData(long id, RandomSupport random) {
            this(
                    id,
                    random.nextInt(3),
                    random.nextInt(10),
                    random.nextInt(10),
                    new HashSet<>(),
                    IntSets.mutable.empty());
        }
    }

    RelationshipVisitorWithProperties<RuntimeException> collector(Set<RelData> into) {
        return (id, type, start, end, addedProps, removedProperties) -> assertThat(into.add(new RelData(
                        id, type, start, end, Iterables.asSet(addedProps), IntSets.mutable.ofAll(removedProperties))))
                .isTrue();
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
                    state.indexDoUpdateEntry(descriptor, nodeId, null, ValueTuple.of(valueAfter));
                }
            }
        };
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
                        (NodeStateModifier)
                                (state, nodeId) -> state.nodeDoAddProperty(nodeId, 42, Values.stringValue("changed")),
                        true),
                Arguments.of(
                        (NodeStateModifier)
                                (state, nodeId) -> state.nodeDoRemoveProperty(nodeId, 42, Predicates.ALWAYS_TRUE_INT),
                        true),
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
                            state.nodeDoAddProperty(nodeId, 42, Values.stringValue("changed"));
                            state.nodeDoDelete(nodeId);
                        },
                        false));
    }

    private record RelationshipIsModifiedTest(String description, Consumer<TxState> txStateConsumer, boolean expected) {

        static final long REL_ID = 0;

        @Override
        public String toString() {
            return description;
        }
    }

    private static Stream<RelationshipIsModifiedTest> relationshipIsModifiedTests() {
        return Stream.of(
                new RelationshipIsModifiedTest(
                        "Created relationship isn't modified",
                        state -> {
                            state.relationshipDoCreate(RelationshipIsModifiedTest.REL_ID, 1, 2, 3);
                        },
                        false),
                new RelationshipIsModifiedTest(
                        "Deleted relationship isn't modified",
                        state -> {
                            state.relationshipDoDelete(RelationshipIsModifiedTest.REL_ID, 1, 2, 3);
                        },
                        false),
                new RelationshipIsModifiedTest(
                        "Deleted relationship in this batch isn't modified",
                        state -> {
                            state.relationshipDoCreate(RelationshipIsModifiedTest.REL_ID, 1, 2, 3);
                            state.relationshipDoDeleteAddedInThisBatch(RelationshipIsModifiedTest.REL_ID);
                        },
                        false),
                new RelationshipIsModifiedTest(
                        "Relationship with new property is modified",
                        state -> {
                            state.relationshipDoAddProperty(
                                    RelationshipIsModifiedTest.REL_ID, 1, 2, 3, 4, Values.stringValue("someValue"));
                        },
                        true),
                new RelationshipIsModifiedTest(
                        "Relationship with replaced property is modified",
                        state -> {
                            state.relationshipDoAddProperty(
                                    RelationshipIsModifiedTest.REL_ID, 1, 2, 3, 4, Values.stringValue("someValue"));
                        },
                        true),
                new RelationshipIsModifiedTest(
                        "Relationship with removed property is modified",
                        state -> {
                            state.relationshipDoRemoveProperty(
                                    RelationshipIsModifiedTest.REL_ID, 1, 2, 3, 4, Predicates.ALWAYS_TRUE_INT);
                        },
                        true),
                new RelationshipIsModifiedTest(
                        "Relationship isn't modified after reset",
                        state -> {
                            state.relationshipDoRemoveProperty(
                                    RelationshipIsModifiedTest.REL_ID, 1, 2, 3, 4, Predicates.ALWAYS_TRUE_INT);
                            state.reset();
                        },
                        false),
                new RelationshipIsModifiedTest(
                        "Relationship with removed property is modified",
                        state -> {
                            state.relationshipDoRemoveProperty(
                                    RelationshipIsModifiedTest.REL_ID, 1, 2, 3, 4, Predicates.ALWAYS_TRUE_INT);
                        },
                        true),
                new RelationshipIsModifiedTest("Unknown relationship to tx state isn't modified", state -> {}, false),
                new RelationshipIsModifiedTest(
                        "Unknown relationship to tx state isn't modified (with other relationships created)",
                        state -> {
                            state.relationshipDoCreate(10, 1, 2, 3);
                        },
                        false));
    }
}
