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
package org.neo4j.consistency.checker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.neo4j.internal.helpers.collection.Iterables.first;
import static org.neo4j.internal.helpers.collection.Iterables.last;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;
import static org.neo4j.kernel.impl.index.schema.RangeIndexProvider.DESCRIPTOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Arrays;
import java.util.function.Consumer;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyKeyTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.SchemaConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.ExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.storageengine.util.IdUpdateListener;

class SchemaCheckerTest extends CheckerTestBase {
    private int label1;
    private int label2;
    private int relationshipType1;
    private int relationshipType2;
    private int propertyKey1;
    private int propertyKey2;
    private final int UNUSED = 99;
    private final MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties = IntObjectMaps.mutable.empty();
    private final MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedNodePropertyTypes =
            IntObjectMaps.mutable.empty();
    private final MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties = IntObjectMaps.mutable.empty();
    private final MutableIntObjectMap<MutableIntObjectMap<PropertyTypeSet>> allowedRelationshipPropertyTypes =
            IntObjectMaps.mutable.empty();
    private final String NAME = "name1";
    private final String NAME2 = "name2";

    @Override
    void initialData(KernelTransaction tx) throws KernelException {
        TokenWrite tokenWrite = tx.tokenWrite();
        label1 = tokenWrite.labelGetOrCreateForName("A");
        label2 = tokenWrite.labelGetOrCreateForName("B");
        relationshipType1 = tokenWrite.relationshipTypeGetOrCreateForName("A");
        relationshipType2 = tokenWrite.relationshipTypeGetOrCreateForName("B");
        propertyKey1 = tokenWrite.propertyKeyGetOrCreateForName("A");
        propertyKey2 = tokenWrite.propertyKeyGetOrCreateForName("B");
    }

    @Test
    void shouldReportDuplicateRuleContent() throws Exception {
        // given
        var cursorContext = CursorContext.NULL_CONTEXT;
        try (AutoCloseable ignored = tx()) {
            IndexDescriptor index1 = IndexPrototype.forSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            IndexDescriptor index2 = IndexPrototype.forSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME2)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            schemaStorage.writeSchemaRule(
                    index1, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    index2, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.duplicateRuleContent(any()));
    }

    @Test
    void shouldReportSchemaRuleNotOnline() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.schemaRuleNotOnline(any()));
    }

    @Test
    void shouldReportSchemaRuleForTokenIndexNotOnline() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.uniqueForSchema(
                            SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, TokenIndexProvider.DESCRIPTOR)
                    .withName(NAME)
                    .withIndexType(IndexType.LOOKUP)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.schemaRuleNotOnline(any()));
    }

    @Test
    void shouldReportLabelNotInUse() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(UNUSED, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.labelNotInUse(any()));
    }

    @Test
    void shouldReportRelationshipTypeNotInUse() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forRelType(UNUSED, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.relationshipTypeNotInUse(any()));
    }

    @Test
    void shouldReportPropertyKeyNotInUse() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forRelType(relationshipType1, UNUSED))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.propertyKeyNotInUse(any()));
    }

    @Test
    void shouldReportMissingObligationUniquenessConstraint() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index1 = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext))
                    .withOwningConstraintId(UNUSED);
            schemaStorage.writeSchemaRule(
                    index1, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.missingObligation(any()));
    }

    @Test
    void shouldReportMissingObligationConstraintIndexRule() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            UniquenessConstraintDescriptor constraintDescriptor = ConstraintDescriptorFactory.uniqueForLabel(
                            label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME)
                    .withOwnedIndexId(UNUSED);
            schemaStorage.writeSchemaRule(
                    constraintDescriptor,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.missingObligation(any()));
    }

    @Test
    void shouldReportMalformedSchemaRule() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            SchemaRecord schemaRecord = schemaStore.newRecord();
            schemaStore.getRecordByCursor(
                    index.getId(), schemaRecord, RecordLoad.NORMAL, storeCursors.readCursor(SCHEMA_CURSOR));
            try (var storeCursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
                propertyStore.updateRecord(
                        new PropertyRecord(schemaRecord.getNextProp()),
                        storeCursor,
                        CursorContext.NULL_CONTEXT,
                        storeCursors);
            }
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, SchemaConsistencyReport::malformedSchemaRule);
    }

    @Test
    void shouldReportConstraintIndexRuleNotReferencingBack() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel(
                            label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME)
                    .withOwnedIndexId(index.getId());
            index = index.withOwningConstraintId(UNUSED);
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    uniquenessConstraint,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.constraintIndexRuleNotReferencingBack(any()));
    }

    @Test
    void shouldReportUniquenessConstraintNotReferencingBack() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel(
                            label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME)
                    .withOwnedIndexId(UNUSED);
            index = index.withOwningConstraintId(uniquenessConstraint.getId());
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    uniquenessConstraint,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.uniquenessConstraintNotReferencingBack(any()));
    }

    @Test
    void shouldReportUniquenessConstraintReferencingIndexOfWrongType() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withIndexType(IndexType.TEXT)
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel(
                            IndexType.RANGE, label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME)
                    .withOwnedIndexId(index.getId());
            index = index.withOwningConstraintId(uniquenessConstraint.getId());
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    uniquenessConstraint,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.uniquenessConstraintReferencingIndexOfWrongType(any()));
    }

    @Test
    void shouldReportDuplicateObligationForIndex() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index1 = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            IndexDescriptor index2 = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label2, propertyKey2))
                    .withName(NAME2)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel(
                            label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME)
                    .withOwnedIndexId(index1.getId());
            index1 = index1.withOwningConstraintId(uniquenessConstraint.getId());
            index2 = index2.withOwningConstraintId(uniquenessConstraint.getId());
            schemaStorage.writeSchemaRule(
                    index1, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    index2, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    uniquenessConstraint,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.duplicateObligation(any()));
    }

    @Test
    void shouldReportDuplicateObligationForConstraint() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            UniquenessConstraintDescriptor uniquenessConstraint1 = ConstraintDescriptorFactory.uniqueForLabel(
                            label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME)
                    .withOwnedIndexId(index.getId());
            UniquenessConstraintDescriptor uniquenessConstraint2 = ConstraintDescriptorFactory.uniqueForLabel(
                            label2, propertyKey2)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME2)
                    .withOwnedIndexId(index.getId());
            index = index.withOwningConstraintId(uniquenessConstraint1.getId());
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    uniquenessConstraint1,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
            schemaStorage.writeSchemaRule(
                    uniquenessConstraint2,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.duplicateObligation(any()));
    }

    @Test
    void shouldPopulateMandatoryPropertiesMap() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            ExistenceConstraintDescriptor constraint1 = ConstraintDescriptorFactory.existsForLabel(
                            false, label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME);
            ExistenceConstraintDescriptor constraint2 = ConstraintDescriptorFactory.existsForLabel(
                            false, label2, propertyKey1, propertyKey2)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME2);
            ExistenceConstraintDescriptor constraint3 = ConstraintDescriptorFactory.existsForRelType(
                            false, relationshipType1, propertyKey2)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME);
            ExistenceConstraintDescriptor constraint4 = ConstraintDescriptorFactory.existsForRelType(
                            false, relationshipType2, propertyKey1, propertyKey2)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME2);
            schemaStorage.writeSchemaRule(
                    constraint1, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    constraint2, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    constraint3, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    constraint4, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        assertEquals(IntSets.mutable.of(propertyKey1), mandatoryNodeProperties.remove(label1));
        assertEquals(IntSets.mutable.of(propertyKey1, propertyKey2), mandatoryNodeProperties.remove(label2));
        assertTrue(mandatoryNodeProperties.isEmpty());
        assertEquals(IntSets.mutable.of(propertyKey2), mandatoryRelationshipProperties.remove(relationshipType1));
        assertEquals(
                IntSets.mutable.of(propertyKey1, propertyKey2),
                mandatoryRelationshipProperties.remove(relationshipType2));
        assertTrue(mandatoryRelationshipProperties.isEmpty());
    }

    @Test
    void shouldPopulateAllowedTypesMap() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            TypeConstraintDescriptor constraint1 = ConstraintDescriptorFactory.typeForSchema(
                            SchemaDescriptors.forLabel(label1, propertyKey1),
                            PropertyTypeSet.of(SchemaValueType.INTEGER),
                            false)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME);
            TypeConstraintDescriptor constraint2 = ConstraintDescriptorFactory.typeForSchema(
                            SchemaDescriptors.forLabel(label2, propertyKey1),
                            PropertyTypeSet.of(SchemaValueType.STRING),
                            false)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME2);
            TypeConstraintDescriptor constraint3 = ConstraintDescriptorFactory.typeForSchema(
                            SchemaDescriptors.forRelType(relationshipType1, propertyKey1),
                            PropertyTypeSet.of(SchemaValueType.BOOLEAN),
                            false)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME);
            TypeConstraintDescriptor constraint4 = ConstraintDescriptorFactory.typeForSchema(
                            SchemaDescriptors.forRelType(relationshipType2, propertyKey1),
                            PropertyTypeSet.of(SchemaValueType.DATE),
                            false)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME2);
            TypeConstraintDescriptor constraint5 = ConstraintDescriptorFactory.typeForSchema(
                            SchemaDescriptors.forRelType(relationshipType2, propertyKey2),
                            PropertyTypeSet.of(SchemaValueType.FLOAT),
                            false)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME2);
            schemaStorage.writeSchemaRule(
                    constraint1, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    constraint2, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    constraint3, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    constraint4, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    constraint5, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
        }

        // when
        check();

        // then
        assertThat(allowedNodePropertyTypes)
                .isEqualTo(IntObjectMaps.mutable.of(
                        label1, IntObjectMaps.mutable.of(propertyKey1, PropertyTypeSet.of(SchemaValueType.INTEGER)),
                        label2, IntObjectMaps.mutable.of(propertyKey1, PropertyTypeSet.of(SchemaValueType.STRING))));
        assertThat(allowedRelationshipPropertyTypes)
                .isEqualTo(IntObjectMaps.mutable.of(
                        relationshipType1,
                                IntObjectMaps.mutable.of(propertyKey1, PropertyTypeSet.of(SchemaValueType.BOOLEAN)),
                        relationshipType2,
                                IntObjectMaps.mutable.of(
                                        propertyKey1, PropertyTypeSet.of(SchemaValueType.DATE),
                                        propertyKey2, PropertyTypeSet.of(SchemaValueType.FLOAT))));
    }

    @Test
    void shouldReportWhenConstraintIndexHasNoConstraintOwnerReference() throws Exception {
        // given
        try (AutoCloseable ignored = tx()) {
            var cursorContext = CursorContext.NULL_CONTEXT;
            IndexDescriptor index = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(label1, propertyKey1))
                    .withName(NAME)
                    .withIndexProvider(DESCRIPTOR)
                    .materialise(schemaIdGenerator.nextId(cursorContext));
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel(
                            label1, propertyKey1)
                    .withId(schemaIdGenerator.nextId(cursorContext))
                    .withName(NAME)
                    .withOwnedIndexId(index.getId());
            schemaStorage.writeSchemaRule(
                    index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
            schemaStorage.writeSchemaRule(
                    uniquenessConstraint,
                    IdUpdateListener.DIRECT,
                    allocatorProvider,
                    cursorContext,
                    INSTANCE,
                    storeCursors);
        }

        // when
        check();

        // then
        expect(SchemaConsistencyReport.class, report -> report.constraintIndexRuleNotReferencingBack(any()));
    }

    @Test
    void shouldReportLabelTokenDynamicRecordNotInUse() throws Exception {
        testDynamicLabelTokenChainInconsistency(
                record -> first(record.getNameRecords()).setInUse(false),
                LabelTokenConsistencyReport.class,
                report -> report.nameBlockNotInUse(any()));
    }

    @Test
    void shouldReportNextLabelTokenDynamicRecordNotInUse() throws Exception {
        testDynamicLabelTokenChainInconsistency(
                record -> last(record.getNameRecords()).setInUse(false),
                DynamicConsistencyReport.class,
                report -> report.nextNotInUse(any()));
    }

    @Test
    void shouldReportDynamicLabelStringEmpty() throws Exception {
        testDynamicLabelTokenChainInconsistency(
                record -> first(record.getNameRecords()).setData(new byte[0]),
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::emptyBlock);
    }

    @Test
    void shouldReportDynamicLabelStringRecordNotFullReferencesNext() throws Exception {
        testDynamicLabelTokenChainInconsistency(
                record -> {
                    DynamicRecord first = first(record.getNameRecords());
                    first(record.getNameRecords()).setData(Arrays.copyOf(first.getData(), first.getLength() / 2));
                },
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::recordNotFullReferencesNext);
    }

    @Test
    void shouldReportPropertyKeyTokenDynamicRecordNotInUse() throws Exception {
        testDynamicPropertyKeyTokenChainInconsistency(
                record -> first(record.getNameRecords()).setInUse(false),
                PropertyKeyTokenConsistencyReport.class,
                report -> report.nameBlockNotInUse(any()));
    }

    @Test
    void shouldReportNextPropertyKeyTokenDynamicRecordNotInUse() throws Exception {
        testDynamicPropertyKeyTokenChainInconsistency(
                record -> last(record.getNameRecords()).setInUse(false),
                DynamicConsistencyReport.class,
                report -> report.nextNotInUse(any()));
    }

    @Test
    void shouldReportDynamicPropertyKeyStringEmpty() throws Exception {
        testDynamicPropertyKeyTokenChainInconsistency(
                record -> first(record.getNameRecords()).setData(new byte[0]),
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::emptyBlock);
    }

    @Test
    void shouldReportDynamicPropertyKeyStringRecordNotFullReferencesNext() throws Exception {
        testDynamicPropertyKeyTokenChainInconsistency(
                record -> {
                    DynamicRecord first = first(record.getNameRecords());
                    first(record.getNameRecords()).setData(Arrays.copyOf(first.getData(), first.getLength() / 2));
                },
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::recordNotFullReferencesNext);
    }

    @Test
    void shouldReportRelationshipTypeTokenDynamicRecordNotInUse() throws Exception {
        testDynamicRelationshipTypeTokenChainInconsistency(
                record -> first(record.getNameRecords()).setInUse(false),
                RelationshipTypeConsistencyReport.class,
                report -> report.nameBlockNotInUse(any()));
    }

    @Test
    void shouldReportNextRelationshipTypeTokenDynamicRecordNotInUse() throws Exception {
        testDynamicRelationshipTypeTokenChainInconsistency(
                record -> last(record.getNameRecords()).setInUse(false),
                DynamicConsistencyReport.class,
                report -> report.nextNotInUse(any()));
    }

    @Test
    void shouldReportDynamicRelationshipTypeStringEmpty() throws Exception {
        testDynamicRelationshipTypeTokenChainInconsistency(
                record -> first(record.getNameRecords()).setData(new byte[0]),
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::emptyBlock);
    }

    @Test
    void shouldReportDynamicRelationshipTypeStringRecordNotFullReferencesNext() throws Exception {
        testDynamicRelationshipTypeTokenChainInconsistency(
                record -> {
                    DynamicRecord first = first(record.getNameRecords());
                    first(record.getNameRecords()).setData(Arrays.copyOf(first.getData(), first.getLength() / 2));
                },
                DynamicConsistencyReport.class,
                DynamicConsistencyReport::recordNotFullReferencesNext);
    }

    private <R extends ConsistencyReport> void testDynamicRelationshipTypeTokenChainInconsistency(
            Consumer<RelationshipTypeTokenRecord> vandal, Class<R> reportClass, Consumer<R> report) throws Exception {
        testDynamicTokenChainInconsistency(
                neoStores.getRelationshipTypeTokenStore(),
                TokenWrite::relationshipTypeGetOrCreateForName,
                vandal,
                reportClass,
                report);
    }

    private <R extends ConsistencyReport> void testDynamicLabelTokenChainInconsistency(
            Consumer<LabelTokenRecord> vandal, Class<R> reportClass, Consumer<R> report) throws Exception {
        testDynamicTokenChainInconsistency(
                neoStores.getLabelTokenStore(), TokenWrite::labelGetOrCreateForName, vandal, reportClass, report);
    }

    private <R extends ConsistencyReport> void testDynamicPropertyKeyTokenChainInconsistency(
            Consumer<PropertyKeyTokenRecord> vandal, Class<R> reportClass, Consumer<R> report) throws Exception {
        testDynamicTokenChainInconsistency(
                neoStores.getPropertyKeyTokenStore(),
                TokenWrite::propertyKeyGetOrCreateForName,
                vandal,
                reportClass,
                report);
    }

    private <TOKEN extends TokenRecord, R extends ConsistencyReport> void testDynamicTokenChainInconsistency(
            TokenStore<TOKEN> store,
            TokenCreator tokenCreator,
            Consumer<TOKEN> vandal,
            Class<R> expectedReportClass,
            Consumer<R> report)
            throws Exception {
        // given
        int tokenId;
        try (KernelTransaction ktx = ktx()) {
            tokenId = tokenCreator.createToken(ktx.tokenWrite(), "Broken".repeat(50));
            ktx.commit();
        }

        try (AutoCloseable ignored = tx()) {
            // (T)--->(D)---> (vandalized dynamic value chain)
            TOKEN record = store.newRecord();
            var cursor = store.getTokenStoreCursor(storeCursors);
            store.getRecordByCursor(tokenId, record, RecordLoad.NORMAL, cursor);
            store.ensureHeavy(record, storeCursors);
            vandal.accept(record);
            DynamicStringStore nameStore = store.getNameStore();
            try (var storeCursor = store.getWriteDynamicTokenCursor(storeCursors)) {
                for (DynamicRecord nameRecord : record.getNameRecords()) {
                    nameStore.updateRecord(nameRecord, storeCursor, CursorContext.NULL_CONTEXT, storeCursors);
                }
            }
        }

        // when
        check();

        // then
        expect(expectedReportClass, report);
    }

    private void check() throws Exception {
        new SchemaChecker(context())
                .check(
                        mandatoryNodeProperties,
                        mandatoryRelationshipProperties,
                        allowedNodePropertyTypes,
                        allowedRelationshipPropertyTypes,
                        CursorContext.NULL_CONTEXT,
                        storeCursors);
    }

    interface TokenCreator {
        int createToken(TokenWrite tokenWrite, String name) throws KernelException;
    }
}
