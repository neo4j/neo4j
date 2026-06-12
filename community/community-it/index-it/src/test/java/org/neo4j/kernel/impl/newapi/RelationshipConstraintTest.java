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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.Values.intValue;

import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SemanticSearchSchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedRelationshipTypeInSchemaException;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.storable.Values;

public class RelationshipConstraintTest extends ConstraintTestBase<WriteTestSupport> {

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    @Override
    SchemaDescriptor schemaDescriptor(int tokenId, int... propertyIds) {
        return SchemaDescriptors.forRelType(tokenId, propertyIds);
    }

    @Override
    ConstraintDescriptor uniqueConstraintDescriptor(int tokenId, int... propertyIds) {
        return ConstraintDescriptorFactory.uniqueForSchema(schemaDescriptor(tokenId, propertyIds));
    }

    @Override
    ConstraintDefinition createConstraint(Schema schema, String entityToken, String propertyKey) {
        return schema.constraintFor(RelationshipType.withName(entityToken))
                .assertPropertyIsUnique(propertyKey)
                .create();
    }

    @Override
    int entityTokenId(TokenWrite tokenWrite, String entityToken) throws KernelException {
        return tokenWrite.relationshipTypeGetOrCreateForName(entityToken);
    }

    @Override
    Iterator<ConstraintDescriptor> getConstraintsByEntityToken(SchemaRead schemaRead, int entityTokenId) {
        return schemaRead.constraintsGetForRelationshipType(entityTokenId);
    }

    @Test
    void shouldCheckUniquenessWhenAddingProperties() throws Exception {
        // GIVEN
        long conflicting, notConflicting;
        addConstraints("FOO", "prop");
        try (Transaction tx = graphDb.beginTx()) {
            Node node = tx.createNode();
            RelationshipType type = RelationshipType.withName("FOO");
            Relationship conflict = node.createRelationshipTo(node, type);
            conflicting = conflict.getId();

            Node node2 = tx.createNode();
            Relationship ok = node.createRelationshipTo(node2, RelationshipType.withName("BAR"));
            notConflicting = ok.getId();

            // Existing relationship
            Relationship existing = node.createRelationshipTo(node2, type);
            existing.setProperty("prop", 1337);
            tx.commit();
        }

        int property;
        try (KernelTransaction tx = beginTransaction()) {
            property = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");

            // This is ok, since it will satisfy constraint
            tx.dataWrite().relationshipSetProperty(notConflicting, property, intValue(1337));

            assertThrows(
                    ConstraintValidationException.class,
                    () -> tx.dataWrite().relationshipSetProperty(conflicting, property, intValue(1337)));
            tx.commit();
        }

        // Verify
        try (KernelTransaction tx = beginTransaction();
                RelationshipScanCursor relCursor = tx.cursors().allocateRelationshipScanCursor(tx.cursorContext());
                PropertyCursor propertyCursor =
                        tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
            // Relationship without conflict
            tx.dataRead().singleRelationship(notConflicting, relCursor);
            assertTrue(relCursor.next());
            relCursor.properties(propertyCursor, PropertySelection.selection(property));
            assertTrue(propertyCursor.next());
            assertEquals(propertyCursor.propertyValue(), Values.intValue(1337));
            // Relationship with conflict
            tx.dataRead().singleRelationship(conflicting, relCursor);
            assertTrue(relCursor.next());
            relCursor.properties(propertyCursor, PropertySelection.selection(property));
            assertFalse(propertyCursor.next());
        }
    }

    @Test
    void shouldFailCreateConstraintWithDuplicateLabels() throws KernelException {
        // given
        int relTypeId0, relTypeId1, relTypeId2, relTypeId3, propId;
        try (KernelTransaction tx = beginTransaction()) {
            relTypeId0 = tx.tokenWrite().relationshipTypeGetOrCreateForName("RELATIONSHIP0");
            relTypeId1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("RELATIONSHIP1");
            relTypeId2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("RELATIONSHIP2");
            relTypeId3 = tx.tokenWrite().relationshipTypeGetOrCreateForName("RELATIONSHIP3");
            propId = tx.tokenWrite().propertyKeyGetOrCreateForName("property");
            tx.commit();
        }

        // when
        SemanticSearchSchemaDescriptor descriptor = SchemaDescriptors.forSemanticSearch(
                org.neo4j.common.EntityType.RELATIONSHIP,
                new int[] {relTypeId0, relTypeId1, relTypeId2, relTypeId1, relTypeId3},
                new int[] {propId});
        // then
        try (KernelTransaction tx = beginTransaction()) {
            RepeatedRelationshipTypeInSchemaException e = assertThrows(
                    RepeatedRelationshipTypeInSchemaException.class,
                    () -> tx.schemaWrite().uniquePropertyConstraintCreate(IndexPrototype.forSchema(descriptor)));
            assertThat(e.gqlStatus()).isEqualTo("22N75");
            assertThat(e.statusDescription())
                    .isEqualTo(
                            "error: data exception - constraint contains duplicated tokens. The constraint specified by '()-[:RELATIONSHIP0|RELATIONSHIP1|RELATIONSHIP2|RELATIONSHIP1|RELATIONSHIP3 {property}]-()' includes a label, relationship type, or property key with name 'RELATIONSHIP1' more than once.");
        }
    }

    @Test
    void shouldFailCreateConstraintWithDuplicateProperties() throws KernelException {
        // given
        int relTypeId, propId0, propId1, propId2, propId3;
        try (KernelTransaction tx = beginTransaction()) {
            relTypeId = tx.tokenWrite().relationshipTypeGetOrCreateForName("RELATIONSHIP");
            propId0 = tx.tokenWrite().propertyKeyGetOrCreateForName("property0");
            propId1 = tx.tokenWrite().propertyKeyGetOrCreateForName("property1");
            propId2 = tx.tokenWrite().propertyKeyGetOrCreateForName("property2");
            propId3 = tx.tokenWrite().propertyKeyGetOrCreateForName("property3");
            tx.commit();
        }

        // when
        SemanticSearchSchemaDescriptor descriptor = SchemaDescriptors.forSemanticSearch(
                org.neo4j.common.EntityType.RELATIONSHIP,
                new int[] {relTypeId},
                new int[] {propId0, propId1, propId2, propId1, propId3});
        // then
        try (KernelTransaction tx = beginTransaction()) {
            RepeatedPropertyInSchemaException e = assertThrows(
                    RepeatedPropertyInSchemaException.class,
                    () -> tx.schemaWrite().uniquePropertyConstraintCreate(IndexPrototype.forSchema(descriptor)));
            assertThat(e.gqlStatus()).isEqualTo("22N75");
            assertThat(e.statusDescription())
                    .isEqualTo(
                            "error: data exception - constraint contains duplicated tokens. The constraint specified by '()-[:RELATIONSHIP {property0, property1, property2, property1, property3}]-()' includes a label, relationship type, or property key with name 'property1' more than once.");
        }
    }
}
