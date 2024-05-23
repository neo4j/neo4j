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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.keyForSchema;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.nodeKeyForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.uniqueForSchema;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.test.storage.RecordStorageEngineSupport;

class RecordStorageReaderSchemaWithPECTest extends RecordStorageReaderTestBase {
    @Override
    protected RecordStorageEngineSupport.Builder modify(RecordStorageEngineSupport.Builder builder) {
        // Basically temporarily allow PEC and node key constraints here, which is usually is only allowed in enterprise
        // edition
        return builder.constraintSemantics(new StandardConstraintRuleAccessor());
    }

    @Test
    void shouldListAllConstraints() throws Exception {
        // Given
        createUniquenessConstraint(label1, propertyKey);
        createUniquenessConstraint(label2, propertyKey);
        createRelUniquenessConstraint(relType1, propertyKey);
        createRelUniquenessConstraint(relType2, propertyKey);
        createNodeKeyConstraint(label1, otherPropertyKey);
        createNodeKeyConstraint(label2, otherPropertyKey);
        createRelKeyConstraint(relType1, propertyKey);
        createRelKeyConstraint(relType2, otherPropertyKey);

        createNodePropertyExistenceConstraint(label2, propertyKey, false);
        createRelPropertyExistenceConstraint(relType1, propertyKey, false);

        // When
        Set<ConstraintDescriptor> constraints = asSet(storageReader.constraintsGetAll());

        // Then
        int labelId1 = labelId(label1);
        int labelId2 = labelId(label2);
        int relTypeId = relationshipTypeId(relType1);
        int relTypeId2 = relationshipTypeId(relType2);
        int propKeyId = propertyKeyId(propertyKey);
        int propKeyId2 = propertyKeyId(otherPropertyKey);

        assertThat(constraints)
                .contains(
                        uniqueForLabel(labelId1, propKeyId),
                        uniqueForLabel(labelId2, propKeyId),
                        uniqueForSchema(SchemaDescriptors.forRelType(relTypeId, propKeyId)),
                        uniqueForSchema(SchemaDescriptors.forRelType(relTypeId2, propKeyId)),
                        nodeKeyForLabel(labelId1, propKeyId2),
                        nodeKeyForLabel(labelId2, propKeyId2),
                        keyForSchema(SchemaDescriptors.forRelType(relTypeId, propKeyId)),
                        keyForSchema(SchemaDescriptors.forRelType(relTypeId2, propKeyId2)),
                        existsForLabel(false, labelId2, propKeyId),
                        existsForRelType(false, relTypeId, propKeyId));
    }

    @Test
    void shouldListAllConstraintsForLabel() throws Exception {
        // Given
        createNodePropertyExistenceConstraint(label1, propertyKey, false);
        createNodePropertyExistenceConstraint(label2, propertyKey, false);

        createUniquenessConstraint(label1, propertyKey);
        createNodeKeyConstraint(label1, otherPropertyKey);
        createNodeKeyConstraint(label2, otherPropertyKey);

        // When
        Set<ConstraintDescriptor> constraints = asSet(storageReader.constraintsGetForLabel(labelId(label1)));

        // Then
        Set<ConstraintDescriptor> expectedConstraints = asSet(
                uniqueConstraintDescriptor(label1, propertyKey),
                nodeKeyConstraintDescriptor(label1, otherPropertyKey),
                nodePropertyExistenceDescriptor(label1, propertyKey, false));

        assertEquals(expectedConstraints, constraints);
    }

    @Test
    void shouldListAllConstraintsForLabelAndProperty() throws Exception {
        // Given
        createUniquenessConstraint(label2, propertyKey);
        createUniquenessConstraint(label1, otherPropertyKey);
        createNodeKeyConstraint(label1, propertyKey);
        createNodeKeyConstraint(label2, otherPropertyKey);

        createNodePropertyExistenceConstraint(label1, propertyKey, false);
        createNodePropertyExistenceConstraint(label2, propertyKey, false);

        // When
        Set<ConstraintDescriptor> constraints = asSet(storageReader.constraintsGetForSchema(
                SchemaDescriptors.forLabel(labelId(label1), propertyKeyId(propertyKey))));

        // Then
        Set<ConstraintDescriptor> expected = asSet(
                nodeKeyConstraintDescriptor(label1, propertyKey),
                nodePropertyExistenceDescriptor(label1, propertyKey, false));

        assertEquals(expected, constraints);
    }

    @Test
    void shouldListAllConstraintsForRelationshipType() throws Exception {
        // Given
        createRelPropertyExistenceConstraint(relType1, propertyKey, false);
        createRelPropertyExistenceConstraint(relType2, propertyKey, false);
        createRelPropertyExistenceConstraint(relType2, otherPropertyKey, false);

        createRelKeyConstraint(relType1, propertyKey);
        createRelKeyConstraint(relType2, otherPropertyKey);

        createRelUniquenessConstraint(relType1, propertyKey);
        createRelUniquenessConstraint(relType2, otherPropertyKey);

        // When
        Set<ConstraintDescriptor> constraints =
                asSet(storageReader.constraintsGetForRelationshipType(relationshipTypeId(relType2)));

        // Then
        Set<ConstraintDescriptor> expectedConstraints = Iterators.asSet(
                relationshipPropertyExistenceDescriptor(relType2, propertyKey, false),
                relationshipPropertyExistenceDescriptor(relType2, otherPropertyKey, false),
                relKeyConstraintDescriptor(relType2, otherPropertyKey),
                relUniqueConstraintDescriptor(relType2, otherPropertyKey));

        assertEquals(expectedConstraints, constraints);
    }

    @Test
    void shouldListAllConstraintsForRelationshipTypeAndProperty() throws Exception {
        // Given
        createRelPropertyExistenceConstraint(relType1, propertyKey, false);
        createRelPropertyExistenceConstraint(relType1, otherPropertyKey, false);

        createRelPropertyExistenceConstraint(relType2, propertyKey, false);
        createRelPropertyExistenceConstraint(relType2, otherPropertyKey, false);

        createRelKeyConstraint(relType1, propertyKey);
        createRelKeyConstraint(relType1, otherPropertyKey);

        createRelUniquenessConstraint(relType1, propertyKey);
        createRelUniquenessConstraint(relType1, otherPropertyKey);

        // When
        int relTypeId = relationshipTypeId(relType1);
        int propKeyId = propertyKeyId(propertyKey);
        Set<ConstraintDescriptor> constraints =
                asSet(storageReader.constraintsGetForSchema(SchemaDescriptors.forRelType(relTypeId, propKeyId)));

        // Then
        Set<ConstraintDescriptor> expectedConstraints = Iterators.asSet(
                relationshipPropertyExistenceDescriptor(relType1, propertyKey, false),
                relKeyConstraintDescriptor(relType1, propertyKey),
                relUniqueConstraintDescriptor(relType1, propertyKey));

        assertEquals(expectedConstraints, constraints);
    }

    private ConstraintDescriptor uniqueConstraintDescriptor(Label label, String propertyKey) {
        int labelId = labelId(label);
        int propKeyId = propertyKeyId(propertyKey);
        return uniqueForLabel(labelId, propKeyId);
    }

    private ConstraintDescriptor relUniqueConstraintDescriptor(RelationshipType type, String propertyKey) {
        int typeId = relationshipTypeId(type);
        int propKeyId = propertyKeyId(propertyKey);
        return uniqueForSchema(SchemaDescriptors.forRelType(typeId, propKeyId));
    }

    private ConstraintDescriptor nodeKeyConstraintDescriptor(Label label, String propertyKey) {
        int labelId = labelId(label);
        int propKeyId = propertyKeyId(propertyKey);
        return nodeKeyForLabel(labelId, propKeyId);
    }

    private ConstraintDescriptor relKeyConstraintDescriptor(RelationshipType type, String propertyKey) {
        int typeId = relationshipTypeId(type);
        int propKeyId = propertyKeyId(propertyKey);
        return keyForSchema(SchemaDescriptors.forRelType(typeId, propKeyId));
    }

    private ConstraintDescriptor nodePropertyExistenceDescriptor(Label label, String propertyKey, boolean isDependent) {
        int labelId = labelId(label);
        int propKeyId = propertyKeyId(propertyKey);
        return existsForLabel(isDependent, labelId, propKeyId);
    }

    private ConstraintDescriptor relationshipPropertyExistenceDescriptor(
            RelationshipType relType, String propertyKey, boolean isDependent) {
        int relTypeId = relationshipTypeId(relType);
        int propKeyId = propertyKeyId(propertyKey);
        return existsForRelType(isDependent, relTypeId, propKeyId);
    }
}
