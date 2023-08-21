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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.Values.intValue;

import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.storageengine.api.PropertySelection;

public class NodeConstraintTest extends ConstraintTestBase<WriteTestSupport> {

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    @Override
    LabelSchemaDescriptor schemaDescriptor(int tokenId, int... propertyIds) {
        return SchemaDescriptors.forLabel(tokenId, propertyIds);
    }

    @Override
    ConstraintDescriptor uniqueConstraintDescriptor(int tokenId, int... propertyIds) {
        return ConstraintDescriptorFactory.uniqueForLabel(tokenId, propertyIds);
    }

    @Override
    ConstraintDefinition createConstraint(Schema schema, String entityToken, String propertyKey) {
        return schema.constraintFor(label(entityToken))
                .assertPropertyIsUnique(propertyKey)
                .create();
    }

    @Override
    int entityTokenId(TokenWrite tokenWrite, String entityToken) throws KernelException {
        return tokenWrite.labelGetOrCreateForName(entityToken);
    }

    @Override
    Iterator<ConstraintDescriptor> getConstraintsByEntityToken(SchemaRead schemaRead, int entityTokenId) {
        return schemaRead.constraintsGetForLabel(entityTokenId);
    }

    @Test
    void shouldCheckUniquenessWhenAddingLabel() throws Exception {
        // GIVEN
        long nodeConflicting, nodeNotConflicting;
        addConstraints("FOO", "prop");
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node conflict = tx.createNode();
            conflict.setProperty("prop", 1337);
            nodeConflicting = conflict.getId();

            Node ok = tx.createNode();
            ok.setProperty("prop", 42);
            nodeNotConflicting = ok.getId();

            // Existing node
            Node existing = tx.createNode();
            existing.addLabel(Label.label("FOO"));
            existing.setProperty("prop", 1337);
            tx.commit();
        }

        int label;
        try (KernelTransaction tx = beginTransaction()) {
            label = tx.tokenWrite().labelGetOrCreateForName("FOO");

            // This is ok, since it will satisfy constraint
            assertTrue(tx.dataWrite().nodeAddLabel(nodeNotConflicting, label));

            assertThrows(
                    ConstraintValidationException.class, () -> tx.dataWrite().nodeAddLabel(nodeConflicting, label));
            tx.commit();
        }

        // Verify
        try (KernelTransaction tx = beginTransaction();
                NodeCursor nodeCursor = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
            // Node without conflict
            tx.dataRead().singleNode(nodeNotConflicting, nodeCursor);
            assertTrue(nodeCursor.next());
            assertTrue(nodeCursor.labels().contains(label));
            // Node with conflict
            tx.dataRead().singleNode(nodeConflicting, nodeCursor);
            assertTrue(nodeCursor.next());
            assertFalse(nodeCursor.labels().contains(label));
        }
    }

    @Test
    void shouldCheckUniquenessWhenAddingProperties() throws Exception {
        // GIVEN
        long nodeConflicting, nodeNotConflicting;
        addConstraints("FOO", "prop");
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            Node conflict = tx.createNode();
            conflict.addLabel(Label.label("FOO"));
            nodeConflicting = conflict.getId();

            Node ok = tx.createNode();
            ok.addLabel(Label.label("BAR"));
            nodeNotConflicting = ok.getId();

            // Existing node
            Node existing = tx.createNode();
            existing.addLabel(Label.label("FOO"));
            existing.setProperty("prop", 1337);
            tx.commit();
        }

        int property;
        try (KernelTransaction tx = beginTransaction()) {
            property = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");

            // This is ok, since it will satisfy constraint
            tx.dataWrite().nodeSetProperty(nodeNotConflicting, property, intValue(1337));

            assertThrows(ConstraintValidationException.class, () -> tx.dataWrite()
                    .nodeSetProperty(nodeConflicting, property, intValue(1337)));
            tx.commit();
        }

        // Verify
        try (KernelTransaction tx = beginTransaction();
                NodeCursor nodeCursor = tx.cursors().allocateNodeCursor(tx.cursorContext());
                PropertyCursor propertyCursor =
                        tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
            // Node without conflict
            tx.dataRead().singleNode(nodeNotConflicting, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.properties(propertyCursor, PropertySelection.selection(property));
            assertTrue(propertyCursor.next());
            // Node with conflict
            tx.dataRead().singleNode(nodeConflicting, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.properties(propertyCursor, PropertySelection.selection(property));
            assertFalse(propertyCursor.next());
        }
    }
}
