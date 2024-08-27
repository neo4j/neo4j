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
package org.neo4j.kernel.impl.storemigration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.SchemaRule44.ConstraintRuleType.UNIQUE;
import static org.neo4j.storageengine.api.SchemaRule44.ConstraintRuleType.UNIQUE_EXISTS;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.BTREE;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.POINT;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.RANGE;
import static org.neo4j.storageengine.api.SchemaRule44.IndexType.TEXT;

import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.recordstorage.SimpleTokenCreator;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore44Reader;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.RegisteringCreatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

class SchemaStore44MigratorTest {
    private static final String MISSING_REPLACEMENT_MESSAGE =
            "Migration will remove all BTREE indexes and constraints backed by BTREE indexes. "
                    + "To guard against unintentionally removing indexes or constraints, "
                    + "it is recommended for all BTREE indexes or constraints backed by BTREE indexes to have a valid replacement. "
                    + "Indexes can be replaced by RANGE, TEXT or POINT index and constraints can be replaced by constraints backed by RANGE index. "
                    + "Please drop your indexes and constraints or create replacements and retry the migration. "
                    + "The indexes and constraints without replacement are: ";
    private static final String NAME_ONE = "Index one";
    private static final String NAME_TWO = "Index two";
    private static final String NAME_THREE = "Index three";
    private final MutableInt schemaId = new MutableInt();
    private int[] labels;
    private int[] props;
    private TokenHolders tokenHolders;

    @BeforeEach
    void setup() throws KernelException {
        tokenHolders = new TokenHolders(
                new RegisteringCreatingTokenHolder(new SimpleTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY),
                new RegisteringCreatingTokenHolder(new SimpleTokenCreator(), TokenHolder.TYPE_LABEL),
                new RegisteringCreatingTokenHolder(new SimpleTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE));
        var nbrOfTokens = 8;
        labels = new int[nbrOfTokens];
        props = new int[nbrOfTokens];
        for (int i = 0; i < nbrOfTokens; i++) {
            labels[i] = tokenHolders.labelTokens().getOrCreateId("Label" + i);
            props[i] = tokenHolders.propertyKeyTokens().getOrCreateId("prop" + i);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void assertCanMigrateShouldThrowOnNonReplacedBtreeIndex(boolean changingFormatFamily) {
        // Given
        var index = index(BTREE, labels[0], props[0], NAME_ONE);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(index));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, changingFormatFamily, tokenHolders, EmptyMemoryTracker.INSTANCE);

        var e = assertThrows(IllegalStateException.class, schemaStoreMigration44::assertCanMigrate);

        // Then
        assertThat(e)
                .hasMessageContaining(MISSING_REPLACEMENT_MESSAGE)
                .hasMessageContaining(index.userDescription(tokenHolders));
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaRule44.ConstraintRuleType.class,
            names = {"UNIQUE", "UNIQUE_EXISTS"})
    void assertCanMigrateShouldThrowOnNonReplacedBtreeBackedConstraint(SchemaRule44.ConstraintRuleType constraintType) {
        // Given
        var indexConstraint = constraint(constraintType, BTREE, labels[0], props[0], NAME_ONE);
        var index = indexConstraint.index();
        var constraint = indexConstraint.constraint();
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(index, constraint));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        var e = assertThrows(IllegalStateException.class, schemaStoreMigration44::assertCanMigrate);

        // Then
        assertThat(e)
                .hasMessageContaining(MISSING_REPLACEMENT_MESSAGE)
                .hasMessageContaining(constraint.userDescription(tokenHolders));
    }

    @ParameterizedTest
    @MethodSource(value = "nonReplacingConstraintCombinations")
    void assertCanMigrateShouldThrowOnNonReplacedBtreeBackedConstraintWithConstraintOfDifferentTypeOnSameSchema(
            SchemaRule44.ConstraintRuleType btreeConstraint, SchemaRule44.ConstraintRuleType otherConstraint) {
        // Given
        var btree = constraint(btreeConstraint, BTREE, labels[0], props[0], NAME_ONE);
        var rangeNodeKey = constraint(otherConstraint, RANGE, labels[0], props[0], NAME_TWO);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(
                        List.of(btree.index(), btree.constraint(), rangeNodeKey.index(), rangeNodeKey.constraint()));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        var e = assertThrows(IllegalStateException.class, schemaStoreMigration44::assertCanMigrate);

        // Then
        assertThat(e)
                .hasMessageContaining(MISSING_REPLACEMENT_MESSAGE)
                .hasMessageContaining(btree.constraint().userDescription(tokenHolders));
    }

    @Test
    void assertCanMigrateShouldIncludeAllSchemaRulesThatLackReplacementInException() {
        // Given
        var btreeIndex1 = index(BTREE, labels[0], props[0], "btreeIndex1");
        var btreeIndex2 = index(BTREE, labels[1], props[2], "btreeIndex2");
        var btreeConstraintUnique = constraint(UNIQUE, BTREE, labels[2], props[2], "btreeConstraintUnique");
        var btreeConstraintNodeKey = constraint(UNIQUE_EXISTS, BTREE, labels[3], props[3], "btreeConstraintNodeKey");

        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(
                        btreeIndex1,
                        btreeIndex2,
                        btreeConstraintUnique.index,
                        btreeConstraintUnique.constraint,
                        btreeConstraintNodeKey.index,
                        btreeConstraintNodeKey.constraint));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        var e = assertThrows(IllegalStateException.class, schemaStoreMigration44::assertCanMigrate);

        // Then
        assertThat(e)
                .hasMessageContaining(MISSING_REPLACEMENT_MESSAGE)
                .hasMessageContaining(btreeIndex1.userDescription(tokenHolders))
                .hasMessageContaining(btreeIndex2.userDescription(tokenHolders))
                .hasMessageContaining(btreeConstraintUnique.constraint.userDescription(tokenHolders))
                .hasMessageContaining(btreeConstraintNodeKey.constraint.userDescription(tokenHolders))
                .hasMessageNotContaining(btreeConstraintUnique.index.userDescription(tokenHolders))
                .hasMessageNotContaining(btreeConstraintNodeKey.index.userDescription(tokenHolders));
    }

    @Test
    void schemaMigrationShouldNotBeAffectedByExistsConstraint() throws KernelException {
        // Given
        var btreeIndexReplacedByRange = index(BTREE, labels[0], props[0], "btreeIndexReplacedByRange");
        var replacingRangeIndex = index(RANGE, labels[0], props[0], "replacingRangeIndex");
        var existsConstraintOnIndexSchema = existsConstraint(labels[0], props[0], "existsConstraintOnIndexSchema");

        // exists constraint on same schema as replaced constraint
        var btreeConstraintUnique = constraint(UNIQUE, BTREE, labels[1], props[1], "btreeConstraintUnique");
        var replacingRangeConstraintUnique =
                constraint(UNIQUE, RANGE, labels[1], props[1], "replacingRangeConstraintUnique");
        var existsConstraintOnConstraintSchema =
                existsConstraint(labels[1], props[1], "existsConstraintOnConstraintSchema");

        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(
                        btreeIndexReplacedByRange,
                        replacingRangeIndex,
                        existsConstraintOnIndexSchema,
                        btreeConstraintUnique.index,
                        btreeConstraintUnique.constraint,
                        replacingRangeConstraintUnique.index,
                        replacingRangeConstraintUnique.constraint,
                        existsConstraintOnConstraintSchema));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).deleteSchemaRule(btreeIndexReplacedByRange.id());
        verify(access).deleteSchemaRule(btreeConstraintUnique.index.id());
        verify(access).deleteSchemaRule(btreeConstraintUnique.constraint.id());
        verifyNoMoreInteractions(access);
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaRule44.IndexType.class,
            names = {"RANGE", "POINT", "TEXT"})
    void schemaMigrationShouldRemoveBtreeIndexIfReplaced(SchemaRule44.IndexType indexType) throws KernelException {
        // Given
        var btree = index(BTREE, labels[0], props[0], NAME_ONE);
        var range = index(indexType, labels[0], props[0], NAME_TWO);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(btree, range));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).deleteSchemaRule(btree.id());
        verifyNoMoreInteractions(access);
    }

    @Test
    void schemaMigrationShouldAddIndexesAndNotRemoveIfChangingFamily() throws KernelException {
        // Given
        var btree = index(BTREE, labels[0], props[0], NAME_ONE);
        var range = index(RANGE, labels[0], props[0], NAME_TWO);
        var text = index(TEXT, labels[0], props[1], NAME_THREE);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(btree, range, text));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, true, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).writeSchemaRule(range.convertTo50rule());
        verify(access).writeSchemaRule(text.convertTo50rule());
        verifyNoMoreInteractions(access);
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaRule44.ConstraintRuleType.class,
            names = {"UNIQUE", "UNIQUE_EXISTS"})
    void schemaMigrationShouldRemoveBtreeConstraintsIfReplaced(SchemaRule44.ConstraintRuleType constraintType)
            throws KernelException {
        // Given
        var btreeUnique = constraint(constraintType, BTREE, labels[0], props[0], NAME_ONE);
        var rangeUnique = constraint(constraintType, RANGE, labels[0], props[0], NAME_TWO);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(
                        btreeUnique.index(), btreeUnique.constraint(), rangeUnique.index(), rangeUnique.constraint()));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).deleteSchemaRule(btreeUnique.index().id());
        verify(access).deleteSchemaRule(btreeUnique.constraint().id());
        verifyNoMoreInteractions(access);
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaRule44.ConstraintRuleType.class,
            names = {"UNIQUE", "UNIQUE_EXISTS"})
    void assertCanMigrateShouldFailIfOrphanedUniqueBtreeIndexExists(SchemaRule44.ConstraintRuleType constraintType) {
        // Given a unique index without a constraint and a RANGE constraint on the same schema
        var btree = uniqueIndex(BTREE, labels[0], props[0], NAME_ONE, 1);
        var rangeUnique = constraint(constraintType, RANGE, labels[0], props[0], NAME_TWO);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(btree, rangeUnique.index(), rangeUnique.constraint()));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        var e = assertThrows(IllegalStateException.class, schemaStoreMigration44::assertCanMigrate);

        // Then we still fail since we can't be sure that the constraint was a replacement for the orphaned index
        assertThat(e)
                .hasMessageContaining(MISSING_REPLACEMENT_MESSAGE)
                .hasMessageContaining(btree.userDescription(tokenHolders));
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
    }

    @Test
    void schemaMigrationShouldRemoveMultipleIndexesAndConstraints() throws KernelException {
        // Given
        var btreeIndexReplacedByRange = index(BTREE, labels[0], props[0], "btreeIndexReplacedByRange");
        var replacingRangeIndex = index(RANGE, labels[0], props[0], "replacingRangeIndex");

        var btreeIndexReplacedByText = index(BTREE, labels[1], props[1], "btreeIndexReplacedByText");
        var replacingTextIndex = index(TEXT, labels[1], props[1], "replacingTextIndex");

        var btreeIndexReplacedByPoint = index(BTREE, labels[2], props[2], "btreeIndexReplacedByPoint");
        var replacingPointIndex = index(POINT, labels[2], props[2], "replacingPointIndex");

        var rangeIndex = index(RANGE, labels[3], props[3], "rangeIndex");
        var textIndex = index(TEXT, labels[3], props[3], "textIndex");
        var pointIndex = index(POINT, labels[3], props[3], "pointIndex");

        var btreeConstraintUnique = constraint(UNIQUE, BTREE, labels[4], props[4], "btreeConstraintUnique");
        var replacingRangeConstraintUnique =
                constraint(UNIQUE, RANGE, labels[4], props[4], "replacingRangeConstraintUnique");

        var btreeConstraintNodeKey = constraint(UNIQUE_EXISTS, BTREE, labels[5], props[5], "btreeConstraintNodeKey");
        var replacingRangeConstraintNodeKey =
                constraint(UNIQUE_EXISTS, RANGE, labels[5], props[5], "replacingRangeConstraintNodeKey");

        var rangeConstraintUnique = constraint(UNIQUE, RANGE, labels[6], props[6], "rangeConstraintUnique");
        var rangeConstraintNodeKey = constraint(UNIQUE_EXISTS, RANGE, labels[7], props[7], "rangeConstraintNodeKey");

        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(
                        btreeIndexReplacedByRange,
                        replacingRangeIndex,
                        btreeIndexReplacedByText,
                        replacingTextIndex,
                        btreeIndexReplacedByPoint,
                        replacingPointIndex,
                        rangeIndex,
                        textIndex,
                        pointIndex,
                        btreeConstraintUnique.index,
                        btreeConstraintUnique.constraint,
                        replacingRangeConstraintUnique.index,
                        replacingRangeConstraintUnique.constraint,
                        btreeConstraintNodeKey.index,
                        btreeConstraintNodeKey.constraint,
                        replacingRangeConstraintNodeKey.index,
                        replacingRangeConstraintNodeKey.constraint,
                        rangeConstraintUnique.index,
                        rangeConstraintUnique.constraint,
                        rangeConstraintNodeKey.index,
                        rangeConstraintNodeKey.constraint));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).deleteSchemaRule(btreeIndexReplacedByRange.id());
        verify(access).deleteSchemaRule(btreeIndexReplacedByText.id());
        verify(access).deleteSchemaRule(btreeIndexReplacedByPoint.id());
        verify(access).deleteSchemaRule(btreeConstraintUnique.index.id());
        verify(access).deleteSchemaRule(btreeConstraintUnique.constraint.id());
        verify(access).deleteSchemaRule(btreeConstraintNodeKey.index.id());
        verify(access).deleteSchemaRule(btreeConstraintNodeKey.constraint.id());
        verifyNoMoreInteractions(access);
    }

    @Test
    void assertCanMigrateWithForceShouldNotThrowOnNonReplacedBtreeIndex() {
        // Given
        var index = index(BTREE, labels[0], props[0], NAME_ONE);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(index));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, true, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        // Then
        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);
    }

    @Test
    void schemaMigrationWithForceShouldReplaceNonUniqueBtreeIndex() throws KernelException {
        // Given
        var index = index(BTREE, labels[0], props[0], NAME_ONE);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(access.nextId()).then(answerNextId());
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(index));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, true, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        ArgumentCaptor<SchemaRule> argument = ArgumentCaptor.forClass(SchemaRule.class);
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).nextId();
        verify(access).deleteSchemaRule(index.id());
        verify(access).writeSchemaRule(argument.capture());
        verifyRangeIndex(index, argument.getValue());
        verifyNoMoreInteractions(access);
        verifyNoMoreInteractions(reader);
    }

    @Test
    void schemaMigrationWithForceShouldNotReplaceNonUniqueBtreeIndexThatHasReplacement() throws KernelException {
        // Given
        var btreeIndex = index(BTREE, labels[0], props[0], NAME_ONE);
        var rangeIndex = index(RANGE, labels[0], props[0], NAME_TWO);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(access.nextId()).then(answerNextId());
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(btreeIndex, rangeIndex));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, true, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).deleteSchemaRule(btreeIndex.id());
        verifyNoMoreInteractions(access);
        verifyNoMoreInteractions(reader);
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaRule44.ConstraintRuleType.class,
            names = {"UNIQUE", "UNIQUE_EXISTS"})
    void schemaMigrationWithForceShouldReplaceBtreeConstraint(SchemaRule44.ConstraintRuleType constraintType)
            throws KernelException {
        // Given
        var btreeUnique = constraint(constraintType, BTREE, labels[0], props[0], NAME_ONE);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(access.nextId()).then(answerNextId());
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(btreeUnique.index(), btreeUnique.constraint()));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, true, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        ArgumentCaptor<SchemaRule> argument = ArgumentCaptor.forClass(SchemaRule.class);
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access, times(2)).nextId();
        verify(access).deleteSchemaRule(btreeUnique.index.id());
        verify(access).deleteSchemaRule(btreeUnique.constraint.id());
        verify(access, times(2)).writeSchemaRule(argument.capture());
        NewConstraintPair rangeUnique = captureNewConstraint(argument);
        verifyConstraintAndIndex(btreeUnique.index, btreeUnique.constraint, rangeUnique.index, rangeUnique.constraint);
        verifyNoMoreInteractions(access);
        verifyNoMoreInteractions(reader);
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaRule44.ConstraintRuleType.class,
            names = {"UNIQUE", "UNIQUE_EXISTS"})
    void schemaMigrationWithForceShouldNotReplaceBtreeConstraintThatAlreadyHasReplacement(
            SchemaRule44.ConstraintRuleType constraintType) throws KernelException {
        // Given
        var btreeUnique = constraint(constraintType, BTREE, labels[0], props[0], NAME_ONE);
        var rangeUnique = constraint(constraintType, RANGE, labels[0], props[0], NAME_TWO);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(access.nextId()).then(answerNextId());
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(
                        btreeUnique.index(), btreeUnique.constraint(), rangeUnique.index, rangeUnique.constraint));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, true, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);
        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verify(access).deleteSchemaRule(btreeUnique.index.id());
        verify(access).deleteSchemaRule(btreeUnique.constraint.id());
        verifyNoMoreInteractions(access);
        verifyNoMoreInteractions(reader);
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaRule44.ConstraintRuleType.class,
            names = {"UNIQUE", "UNIQUE_EXISTS"})
    void schemaMigrationWithForceShouldNotTouchRangeSchemas(SchemaRule44.ConstraintRuleType constraintType)
            throws KernelException {
        // Given
        var rangeIndex = index(RANGE, labels[0], props[0], NAME_ONE);
        var rangeUnique = constraint(constraintType, RANGE, labels[1], props[1], NAME_TWO);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(access.nextId()).then(answerNextId());
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(rangeIndex, rangeUnique.index(), rangeUnique.constraint()));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, true, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertDoesNotThrow(schemaStoreMigration44::assertCanMigrate);

        schemaStoreMigration44.migrate(access, tokenHolders);
        // Then
        verify(reader).loadAllSchemaRules(any(StoreCursors.class), any());
        verifyNoMoreInteractions(access);
        verifyNoMoreInteractions(reader);
    }

    @Test
    void shouldPersistNodeLabelIndexWhenLoadingFormerLabelScanStoreWithoutId() throws KernelException {
        // Given
        var id = 15L;
        var formerLabelScanStoreWithoutId = SchemaStore44Reader.constructFormerLabelScanStoreSchemaRule();
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any()))
                .thenReturn(List.of(formerLabelScanStoreWithoutId));
        when(access.nextId()).thenReturn(id);

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertThat(schemaStoreMigration44.existingSchemaRulesToAdd())
                .containsExactly(formerLabelScanStoreWithoutId.convertTo50rule());

        schemaStoreMigration44.assertCanMigrate();

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(access).nextId();
        verify(access).writeSchemaRule(SchemaStore44Migration.NLI_PROTOTYPE.materialise(id));
        verifyNoMoreInteractions(access);
    }

    @Test
    void shouldPersistNodeLabelIndexWhenLoadingFormerLabelScanStoreWithId() throws KernelException {
        // Given
        var id = 15L;
        var formerLabelScanStoreWithId = SchemaStore44Reader.constructFormerLabelScanStoreSchemaRule(id);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(formerLabelScanStoreWithId));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertThat(schemaStoreMigration44.existingSchemaRulesToAdd())
                .containsExactly(formerLabelScanStoreWithId.convertTo50rule());

        schemaStoreMigration44.assertCanMigrate();

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(access).writeSchemaRule(SchemaStore44Migration.NLI_PROTOTYPE.materialise(id));
        verifyNoMoreInteractions(access);
    }

    @Test
    void shouldNotPersistNodeLabelIndexWhenLoadingExistingNodeLabelIndex() throws KernelException {
        // Given
        var existingNli = new SchemaRule44.Index(
                15L,
                SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR,
                false,
                "My nli",
                SchemaRule44.IndexType.LOOKUP,
                new IndexProviderDescriptor("token-lookup", "1.0"),
                IndexConfig.empty(),
                null);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(existingNli));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, false, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertThat(schemaStoreMigration44.existingSchemaRulesToAdd()).isEmpty();

        schemaStoreMigration44.assertCanMigrate();

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verifyNoInteractions(access);
    }

    @Test
    void shouldPersistNodeLabelIndexWhenLoadingExistingNodeLabelIndexIfSwitchingFamily() throws KernelException {
        // Given
        var existingNli = new SchemaRule44.Index(
                15L,
                SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR,
                false,
                "My nli",
                SchemaRule44.IndexType.LOOKUP,
                new IndexProviderDescriptor("token-lookup", "1.0"),
                IndexConfig.empty(),
                null);
        var reader = mock(SchemaStore44Reader.class);
        var storeCursors = mock(StoreCursors.class);
        var access = mock(SchemaRuleMigrationAccess.class);
        when(reader.loadAllSchemaRules(any(StoreCursors.class), any())).thenReturn(List.of(existingNli));

        // When
        SchemaStore44Migration.SchemaStore44Migrator schemaStoreMigration44 =
                SchemaStore44Migration.getSchemaStoreMigration44(
                        reader, storeCursors, false, true, tokenHolders, EmptyMemoryTracker.INSTANCE);

        assertThat(schemaStoreMigration44.existingSchemaRulesToAdd()).containsExactly(existingNli.convertTo50rule());

        schemaStoreMigration44.assertCanMigrate();

        schemaStoreMigration44.migrate(access, tokenHolders);

        // Then
        verify(access).writeSchemaRule(existingNli.convertTo50rule());
        verifyNoMoreInteractions(access);
    }

    private static void verifyRangeIndex(SchemaRule44.Index index, SchemaRule newSchemaRule) {
        assertThat(newSchemaRule).isInstanceOf(IndexDescriptor.class);
        var rangeIndex = (IndexDescriptor) newSchemaRule;
        assertThat(rangeIndex.getId()).isNotEqualTo(index.id());
        assertThat(rangeIndex.getName()).isEqualTo(index.name());
        assertThat(rangeIndex.schema()).isEqualTo(index.schema());
        assertThat(rangeIndex.getIndexType()).isEqualTo(IndexType.RANGE);
        assertThat(rangeIndex.isUnique()).isEqualTo(index.unique());
        assertThat(rangeIndex.getIndexProvider()).isEqualTo(new IndexProviderDescriptor("range", "1.0"));
        if (index.owningConstraintId() == null) {
            assertThat(rangeIndex.getOwningConstraintId()).isEmpty();
        } else {
            assertThat(rangeIndex.getOwningConstraintId().isPresent()).isTrue();
            assertThat(rangeIndex.getOwningConstraintId().getAsLong()).isNotEqualTo(index.owningConstraintId());
        }
    }

    private static void verifyConstraintAndIndex(
            SchemaRule44.Index index,
            SchemaRule44.Constraint constraint,
            IndexDescriptor newIndex,
            ConstraintDescriptor newConstraint) {
        verifyRangeIndex(index, newIndex);
        verifyConstraint(constraint, newConstraint);
        verifyConnected(newIndex, newConstraint);
    }

    private static void verifyConnected(IndexDescriptor newIndex, ConstraintDescriptor newConstraint) {
        if (newConstraint.isIndexBackedConstraint()) {
            var newConstraintIndexBacked = newConstraint.asIndexBackedConstraint();
            assertThat(newConstraintIndexBacked.ownedIndexId()).isEqualTo(newIndex.getId());
            assertThat(newIndex.getOwningConstraintId().isPresent()).isTrue();
            assertThat(newIndex.getOwningConstraintId().getAsLong()).isEqualTo(newConstraint.getId());
        }
    }

    private static void verifyConstraint(SchemaRule44.Constraint constraint, ConstraintDescriptor newConstraint) {
        assertThat(newConstraint.schema()).isEqualTo(constraint.schema());
        assertThat(newConstraint.type())
                .isEqualTo(constraint.constraintRuleType().asConstraintType());
        assertThat(newConstraint.getId()).isNotEqualTo(constraint.id());
        assertThat(newConstraint.getName()).isEqualTo(constraint.name());
        assertThat(newConstraint.isIndexBackedConstraint())
                .isEqualTo(constraint.constraintRuleType().isIndexBacked());
    }

    private SchemaRule44.Index index(SchemaRule44.IndexType indexType, int label, int property, String name) {
        var labelSchemaDescriptor = SchemaDescriptors.forLabel(label, property);
        return new SchemaRule44.Index(
                schemaId.getAndIncrement(),
                labelSchemaDescriptor,
                false,
                name,
                indexType,
                IndexProviderDescriptor.UNDECIDED,
                IndexConfig.empty(),
                null);
    }

    private SchemaRule44.Index uniqueIndex(
            SchemaRule44.IndexType indexType, int label, int property, String name, long constraintId) {
        var labelSchemaDescriptor = SchemaDescriptors.forLabel(label, property);
        return new SchemaRule44.Index(
                schemaId.getAndIncrement(),
                labelSchemaDescriptor,
                true,
                name,
                indexType,
                IndexProviderDescriptor.UNDECIDED,
                IndexConfig.empty(),
                constraintId);
    }

    private ConstraintPair constraint(
            SchemaRule44.ConstraintRuleType constraintType,
            SchemaRule44.IndexType indexType,
            int label,
            int property,
            String name) {
        assertThat(constraintType.isIndexBacked()).isTrue();
        var constraintId = schemaId.getAndIncrement();
        var index = uniqueIndex(indexType, label, property, name, constraintId);
        var constraint = new SchemaRule44.Constraint(
                constraintId, index.schema(), name, constraintType, index.id(), index.indexType());
        return new ConstraintPair(index, constraint);
    }

    private SchemaRule44.Constraint existsConstraint(int label, int property, String name) {
        var schema = SchemaDescriptors.forLabel(label, property);
        return new SchemaRule44.Constraint(
                schemaId.getAndIncrement(), schema, name, SchemaRule44.ConstraintRuleType.EXISTS, null, null);
    }

    private NewConstraintPair captureNewConstraint(ArgumentCaptor<SchemaRule> argument) {
        IndexDescriptor newIndex = null;
        ConstraintDescriptor newConstraint = null;
        for (SchemaRule newSchemaRule : argument.getAllValues()) {
            if (newSchemaRule instanceof IndexDescriptor index) {
                newIndex = index;
            }
            if (newSchemaRule instanceof ConstraintDescriptor constraint) {
                newConstraint = constraint;
            }
        }
        return new NewConstraintPair(newIndex, newConstraint);
    }

    private Answer<Object> answerNextId() {
        return invocationOnMock -> (long) schemaId.getAndIncrement();
    }

    private static Stream<Arguments> nonReplacingConstraintCombinations() {
        return Stream.of(Arguments.of(UNIQUE, UNIQUE_EXISTS), Arguments.of(UNIQUE_EXISTS, UNIQUE));
    }

    private record ConstraintPair(SchemaRule44.Index index, SchemaRule44.Constraint constraint) {}

    private record NewConstraintPair(IndexDescriptor index, ConstraintDescriptor constraint) {}
}
