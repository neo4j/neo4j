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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.assertCounts;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.computeKey;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.count;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.sparse;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.kernel.api.security.TestAccessMode;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.storable.ValueGroup;

@SuppressWarnings("Duplicates")
public abstract class RelationshipTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G> {
    @Test
    void shouldSeeSingleRelationshipInTransaction() throws Exception {
        int label;
        long n1, n2;
        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            // setup extra relationship to challenge the implementation
            long decoyNode = tx.dataWrite().nodeCreate();
            label = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            tx.dataWrite().relationshipCreate(n2, label, decoyNode);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            long r = tx.dataWrite().relationshipCreate(n1, label, n2);
            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                tx.dataRead().singleRelationship(r, relationship);
                assertTrue(relationship.next(), "should find relationship");

                assertEquals(label, relationship.type());
                assertEquals(n1, relationship.sourceNodeReference());
                assertEquals(n2, relationship.targetNodeReference());
                assertEquals(r, relationship.relationshipReference());

                assertFalse(relationship.next(), "should only find one relationship");
            }
            tx.commit();
        }
    }

    @Test
    void shouldNotSeeSingleRelationshipWhichWasDeletedInTransaction() throws Exception {
        int label;
        long n1, n2, r;
        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            label = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            long decoyNode = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate(n2, label, decoyNode); // to have >1 relationship in the db

            r = tx.dataWrite().relationshipCreate(n1, label, n2);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            assertTrue(tx.dataWrite().relationshipDelete(r), "should delete relationship");
            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                tx.dataRead().singleRelationship(r, relationship);
                assertFalse(relationship.next(), "should not find relationship");
            }
            tx.commit();
        }
    }

    @Test
    void shouldNotSeeRelationshipsForSingleRelationshipNoId() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            try (var relationships = tx.cursors().allocateRelationshipScanCursor(tx.cursorContext())) {
                final var write = tx.dataWrite();
                final var relType = tx.tokenWrite().relationshipTypeGetOrCreateForName("REL");
                write.relationshipCreate(write.nodeCreate(), relType, write.nodeCreate());

                tx.dataRead().singleRelationship(-1, relationships);
                assertFalse(relationships.next());
            }
        }
    }

    @Test
    void shouldScanRelationshipInTransaction() throws Exception {
        final int nRelationshipsInStore = 10;

        int type;
        long n1, n2;

        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            // setup some in store relationships
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            relateNTimes(nRelationshipsInStore, type, n1, n2, tx);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            long r = tx.dataWrite().relationshipCreate(n1, type, n2);
            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                tx.dataRead().allRelationshipsScan(relationship);
                assertCountRelationships(relationship, nRelationshipsInStore + 1, n1, type, n2);
            }
            tx.commit();
        }
    }

    @Test
    void shouldNotScanRelationshipWhichWasDeletedInTransaction() throws Exception {
        final int nRelationshipsInStore = 5 + 1 + 5;

        int type;
        long n1, n2, r;
        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");

            relateNTimes(5, type, n1, n2, tx);
            r = tx.dataWrite().relationshipCreate(n1, type, n2);
            relateNTimes(5, type, n1, n2, tx);

            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            assertTrue(tx.dataWrite().relationshipDelete(r), "should delete relationship");
            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                tx.dataRead().allRelationshipsScan(relationship);
                assertCountRelationships(relationship, nRelationshipsInStore - 1, n1, type, n2);
            }
            tx.commit();
        }
    }

    @Test
    void shouldSeeRelationshipInTransaction() throws Exception {
        long n1, n2;
        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long r = tx.dataWrite().relationshipCreate(n1, label, n2);
            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor relationship =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                tx.dataRead().singleNode(n1, node);
                assertTrue(node.next(), "should access node");

                node.relationships(relationship, ALL_RELATIONSHIPS);
                assertTrue(relationship.next(), "should find relationship");
                assertEquals(r, relationship.relationshipReference());

                assertFalse(relationship.next(), "should only find one relationship");
            }
            tx.commit();
        }
    }

    @Test
    void shouldNotSeeRelationshipDeletedInTransaction() throws Exception {
        long n1, n2, r;
        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            r = tx.dataWrite().relationshipCreate(n1, label, n2);

            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite().relationshipDelete(r);
            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor relationship =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                tx.dataRead().singleNode(n1, node);
                assertTrue(node.next(), "should access node");

                node.relationships(relationship, ALL_RELATIONSHIPS);
                assertFalse(relationship.next(), "should not find relationship");
            }
            tx.commit();
        }
    }

    @Test
    void shouldSeeRelationshipInTransactionBeforeCursorInitialization() throws Exception {
        long n1, n2;
        try (KernelTransaction tx = beginTransaction()) {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long r = tx.dataWrite().relationshipCreate(n1, label, n2);
            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor relationship =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                tx.dataRead().singleNode(n1, node);
                assertTrue(node.next(), "should access node");

                node.relationships(relationship, ALL_RELATIONSHIPS);
                assertTrue(relationship.next(), "should find relationship");
                assertEquals(r, relationship.relationshipReference());

                tx.dataWrite().relationshipCreate(n1, label, n2); // should not be seen
                assertFalse(relationship.next(), "should not find relationship added after cursor init");
            }
            tx.commit();
        }
    }

    @Test
    void shouldTraverseSparseNode() throws Exception {
        traverse(sparse(graphDb), false);
    }

    @Test
    void shouldTraverseDenseNode() throws Exception {
        traverse(RelationshipTestSupport.dense(graphDb), false);
    }

    @Test
    void shouldTraverseSparseNodeWithDetachedReferences() throws Exception {
        traverse(sparse(graphDb), true);
    }

    @Test
    void shouldTraverseDenseNodeWithDetachedReferences() throws Exception {
        traverse(RelationshipTestSupport.dense(graphDb), true);
    }

    @Test
    void shouldSeeNewRelationshipPropertyInTransaction() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            String propKey1 = "prop1";
            String propKey2 = "prop2";
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long r = tx.dataWrite().relationshipCreate(n1, label, n2);
            int prop1 = tx.token().propertyKeyGetOrCreateForName(propKey1);
            int prop2 = tx.token().propertyKeyGetOrCreateForName(propKey2);
            assertEquals(tx.dataWrite().relationshipSetProperty(r, prop1, stringValue("hello")), NO_VALUE);
            assertEquals(tx.dataWrite().relationshipSetProperty(r, prop2, stringValue("world")), NO_VALUE);

            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor relationship =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT);
                    PropertyCursor property = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
                tx.dataRead().singleNode(n1, node);
                assertTrue(node.next(), "should access node");
                node.relationships(relationship, ALL_RELATIONSHIPS);

                assertTrue(relationship.next(), "should access relationship");

                relationship.properties(property);

                while (property.next()) {
                    if (property.propertyKey() == prop1) {
                        assertEquals(property.propertyValue(), stringValue("hello"));
                    } else if (property.propertyKey() == prop2) {
                        assertEquals(property.propertyValue(), stringValue("world"));
                    } else {
                        fail(property.propertyKey() + " was not the property key you were looking for");
                    }
                }

                assertFalse(relationship.next(), "should only find one relationship");
            }
            tx.commit();
        }
    }

    @Test
    void shouldSeeAddedPropertyFromExistingRelationshipWithoutPropertiesInTransaction() throws Exception {
        // Given
        long relationshipId;
        String propKey = "prop1";
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate(
                    write.nodeCreate(), tx.tokenWrite().relationshipTypeGetOrCreateForName("R"), write.nodeCreate());
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            int propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(
                    tx.dataWrite().relationshipSetProperty(relationshipId, propToken, stringValue("hello")), NO_VALUE);

            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT);
                    PropertyCursor property = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
                tx.dataRead().singleRelationship(relationshipId, relationship);
                assertTrue(relationship.next(), "should access relationship");

                relationship.properties(property);
                assertTrue(property.next());
                assertEquals(propToken, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("hello"));

                assertFalse(property.next(), "should only find one properties");
                assertFalse(relationship.next(), "should only find one relationship");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertThat(transaction.getRelationshipById(relationshipId).getProperty(propKey))
                    .isEqualTo("hello");
        }
    }

    @Test
    void shouldSeeAddedPropertyFromExistingRelationshipWithPropertiesInTransaction() throws Exception {
        // Given
        long relationshipId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";
        int propToken1;
        int propToken2;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate(
                    write.nodeCreate(), tx.tokenWrite().relationshipTypeGetOrCreateForName("R"), write.nodeCreate());
            propToken1 = tx.token().propertyKeyGetOrCreateForName(propKey1);
            assertEquals(write.relationshipSetProperty(relationshipId, propToken1, stringValue("hello")), NO_VALUE);
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            propToken2 = tx.token().propertyKeyGetOrCreateForName(propKey2);
            assertEquals(
                    tx.dataWrite().relationshipSetProperty(relationshipId, propToken2, stringValue("world")), NO_VALUE);

            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT);
                    PropertyCursor property = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
                tx.dataRead().singleRelationship(relationshipId, relationship);
                assertTrue(relationship.next(), "should access relationship");

                relationship.properties(property);

                while (property.next()) {
                    if (property.propertyKey() == propToken1) // from disk
                    {
                        assertEquals(property.propertyValue(), stringValue("hello"));

                    } else if (property.propertyKey() == propToken2) // from tx state
                    {
                        assertEquals(property.propertyValue(), stringValue("world"));
                    } else {
                        fail(property.propertyKey() + " was not the property you were looking for");
                    }
                }

                assertFalse(relationship.next(), "should only find one relationship");
            }
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            Relationship relationship = transaction.getRelationshipById(relationshipId);
            assertThat(relationship.getProperty(propKey1)).isEqualTo("hello");
            assertThat(relationship.getProperty(propKey2)).isEqualTo("world");
        }
    }

    @Test
    void shouldSeeUpdatedPropertyFromExistingRelationshipWithPropertiesInTransaction() throws Exception {
        // Given
        long relationshipId;
        String propKey = "prop1";
        int propToken;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate(
                    write.nodeCreate(), tx.tokenWrite().relationshipTypeGetOrCreateForName("R"), write.nodeCreate());
            propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(write.relationshipSetProperty(relationshipId, propToken, stringValue("hello")), NO_VALUE);
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            assertEquals(
                    tx.dataWrite().relationshipSetProperty(relationshipId, propToken, stringValue("world")),
                    stringValue("hello"));
            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT);
                    PropertyCursor property = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
                tx.dataRead().singleRelationship(relationshipId, relationship);
                assertTrue(relationship.next(), "should access relationship");

                relationship.properties(property);

                assertTrue(property.next());
                assertEquals(propToken, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("world"));

                assertFalse(property.next(), "should only find one property");
                assertFalse(relationship.next(), "should only find one relationship");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertThat(transaction.getRelationshipById(relationshipId).getProperty(propKey))
                    .isEqualTo("world");
        }
    }

    @Test
    void shouldNotSeeRemovedPropertyInTransaction() throws Exception {
        // Given
        long relationshipId;
        String propKey = "prop1";
        int propToken;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate(
                    write.nodeCreate(), tx.tokenWrite().relationshipTypeGetOrCreateForName("R"), write.nodeCreate());
            propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(write.relationshipSetProperty(relationshipId, propToken, stringValue("hello")), NO_VALUE);
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            assertEquals(tx.dataWrite().relationshipRemoveProperty(relationshipId, propToken), stringValue("hello"));
            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT);
                    PropertyCursor property = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
                tx.dataRead().singleRelationship(relationshipId, relationship);
                assertTrue(relationship.next(), "should access relationship");

                relationship.properties(property);
                assertFalse(property.next(), "should not find any properties");
                assertFalse(relationship.next(), "should only find one relationship");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertFalse(transaction.getRelationshipById(relationshipId).hasProperty(propKey));
        }
    }

    @Test
    void shouldSeeRemovedThenAddedPropertyInTransaction() throws Exception {
        // Given
        long relationshipId;
        String propKey = "prop1";
        int propToken;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationshipId = write.relationshipCreate(
                    write.nodeCreate(), tx.tokenWrite().relationshipTypeGetOrCreateForName("R"), write.nodeCreate());
            propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(write.relationshipSetProperty(relationshipId, propToken, stringValue("hello")), NO_VALUE);
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            assertEquals(tx.dataWrite().relationshipRemoveProperty(relationshipId, propToken), stringValue("hello"));
            assertEquals(
                    tx.dataWrite().relationshipSetProperty(relationshipId, propToken, stringValue("world")), NO_VALUE);
            try (RelationshipScanCursor relationship = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT);
                    PropertyCursor property = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
                tx.dataRead().singleRelationship(relationshipId, relationship);
                assertTrue(relationship.next(), "should access relationship");

                relationship.properties(property);
                assertTrue(property.next());
                assertEquals(propToken, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("world"));

                assertFalse(property.next(), "should not find any properties");
                assertFalse(relationship.next(), "should only find one relationship");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction transaction = graphDb.beginTx()) {
            assertThat(transaction.getRelationshipById(relationshipId).getProperty(propKey))
                    .isEqualTo("world");
        }
    }

    @Test
    void shouldCountFromTxState() throws Exception {
        // dense outgoing
        assertCount(100, RelationshipDirection.OUTGOING, degree -> {
            assertEquals(101, degree.outgoingDegree());
            assertEquals(0, degree.incomingDegree());
            assertEquals(101, degree.totalDegree());
        });
        // sparse outgoing
        assertCount(1, RelationshipDirection.OUTGOING, degree -> {
            assertEquals(2, degree.outgoingDegree());
            assertEquals(0, degree.incomingDegree());
            assertEquals(2, degree.totalDegree());
        });
        // dense incoming
        assertCount(100, RelationshipDirection.INCOMING, degree -> {
            assertEquals(0, degree.outgoingDegree());
            assertEquals(101, degree.incomingDegree());
            assertEquals(0, degree.outgoingDegree());
            assertEquals(101, degree.totalDegree());
        });
        // sparse incoming
        assertCount(1, RelationshipDirection.INCOMING, degree -> {
            assertEquals(0, degree.outgoingDegree());
            assertEquals(2, degree.incomingDegree());
            assertEquals(2, degree.totalDegree());
        });

        // dense loops
        assertCount(100, RelationshipDirection.LOOP, degree -> {
            assertEquals(101, degree.incomingDegree());
            assertEquals(101, degree.outgoingDegree());
            assertEquals(101, degree.totalDegree());
        });
        // sparse loops
        assertCount(1, RelationshipDirection.LOOP, degree -> {
            assertEquals(2, degree.outgoingDegree());
            assertEquals(2, degree.incomingDegree());
            assertEquals(2, degree.totalDegree());
        });
    }

    @Test
    void shouldSeeNewTypes() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            long start = write.nodeCreate();
            int outgoing = tx.tokenWrite().relationshipTypeGetOrCreateForName("OUT");
            int incoming = tx.tokenWrite().relationshipTypeGetOrCreateForName("IN");
            int looping = tx.tokenWrite().relationshipTypeGetOrCreateForName("LOOP");
            long out = write.relationshipCreate(start, outgoing, write.nodeCreate());
            long in1 = write.relationshipCreate(write.nodeCreate(), incoming, start);
            long in2 = write.relationshipCreate(write.nodeCreate(), incoming, start);
            long loop = write.relationshipCreate(start, looping, start);
            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor traversal =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                org.neo4j.internal.kernel.api.Read read = tx.dataRead();
                read.singleNode(start, node);
                assertTrue(node.next());
                Degrees degrees = node.degrees(ALL_RELATIONSHIPS);
                for (int t : degrees.types()) {
                    if (t == outgoing) {
                        assertEquals(1, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                        assertRelationships(OUTGOING, node, t, traversal, out);
                        assertNoRelationships(INCOMING, node, t, traversal);
                    } else if (t == incoming) {
                        assertEquals(0, degrees.outgoingDegree(t));
                        assertEquals(2, degrees.incomingDegree(t));
                        assertRelationships(INCOMING, node, t, traversal, in1, in2);
                        assertNoRelationships(OUTGOING, node, t, traversal);
                    } else if (t == looping) {
                        assertEquals(1, degrees.outgoingDegree(t));
                        assertEquals(1, degrees.incomingDegree(t));
                        assertRelationships(BOTH, node, t, traversal, loop);
                        assertRelationships(OUTGOING, node, t, traversal, loop);
                        assertRelationships(INCOMING, node, t, traversal, loop);
                    } else {
                        fail(t + "  is not the type you're looking for ");
                    }
                }
            }
        }
    }

    @Test
    void shouldAddToCountFromTxState() throws Exception {
        long start;
        long existingRelationship;
        int type;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            start = write.nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName("OUT");
            existingRelationship = write.relationshipCreate(start, type, write.nodeCreate());
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            long newRelationship = write.relationshipCreate(start, type, write.nodeCreate());

            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor traversal =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                org.neo4j.internal.kernel.api.Read read = tx.dataRead();
                read.singleNode(start, node);
                assertTrue(node.next());
                Degrees degrees = node.degrees(selection(type, BOTH));
                assertEquals(2, degrees.outgoingDegree(type));
                assertEquals(0, degrees.incomingDegree(type));
                assertRelationships(OUTGOING, node, type, traversal, newRelationship, existingRelationship);
            }
        }
    }

    @Test
    void shouldSeeBothOldAndNewRelationshipsFromSparseNode() throws Exception {
        long start;
        long existingRelationship;
        int one;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            start = write.nodeCreate();
            one = tx.tokenWrite().relationshipTypeGetOrCreateForName("ONE");
            existingRelationship = write.relationshipCreate(start, one, write.nodeCreate());
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int two = tx.tokenWrite().relationshipTypeGetOrCreateForName("TWO");
            long newRelationship = write.relationshipCreate(start, two, write.nodeCreate());

            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor traversal =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                org.neo4j.internal.kernel.api.Read read = tx.dataRead();
                read.singleNode(start, node);
                assertTrue(node.next());
                Degrees degrees = node.degrees(ALL_RELATIONSHIPS);
                for (int t : degrees.types()) {
                    if (t == one) {
                        assertEquals(1, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                        assertRelationships(OUTGOING, node, t, traversal, existingRelationship);
                    } else if (t == two) {
                        assertEquals(1, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                        assertRelationships(OUTGOING, node, t, traversal, newRelationship);

                    } else {
                        fail(t + "  is not the type you're looking for ");
                    }
                }
            }
        }
    }

    @Test
    void shouldSeeBothOldAndNewRelationshipsFromDenseNode() throws Exception {
        long start;
        long existingRelationship;
        int one, bulk;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            start = write.nodeCreate();
            one = tx.tokenWrite().relationshipTypeGetOrCreateForName("ONE");
            existingRelationship = write.relationshipCreate(start, one, write.nodeCreate());
            bulk = tx.tokenWrite().relationshipTypeGetOrCreateForName("BULK");
            for (int i = 0; i < 100; i++) {
                write.relationshipCreate(start, bulk, write.nodeCreate());
            }
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int two = tx.tokenWrite().relationshipTypeGetOrCreateForName("TWO");
            long newRelationship = write.relationshipCreate(start, two, write.nodeCreate());

            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor traversal =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                org.neo4j.internal.kernel.api.Read read = tx.dataRead();
                read.singleNode(start, node);
                assertTrue(node.next());
                assertTrue(node.supportsFastDegreeLookup());
                Degrees degrees = node.degrees(ALL_RELATIONSHIPS);
                for (int t : degrees.types()) {
                    if (t == one) {
                        assertEquals(1, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                        assertRelationships(OUTGOING, node, t, traversal, existingRelationship);

                    } else if (t == two) {
                        assertEquals(1, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                        assertRelationships(OUTGOING, node, t, traversal, newRelationship);
                    } else if (t == bulk) {
                        assertEquals(100, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                    } else {
                        fail(t + "  is not the type you're looking for ");
                    }
                }
            }
        }
    }

    @Test
    void shouldNewRelationshipBetweenAlreadyConnectedSparseNodes() throws Exception {
        long start;
        long end;
        long existingRelationship;
        int type;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            start = write.nodeCreate();
            end = write.nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            existingRelationship = write.relationshipCreate(start, type, end);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            long newRelationship = write.relationshipCreate(start, type, write.nodeCreate());

            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor traversal =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                org.neo4j.internal.kernel.api.Read read = tx.dataRead();
                read.singleNode(start, node);
                assertTrue(node.next());
                Degrees degrees = node.degrees(selection(type, BOTH));
                assertEquals(2, degrees.outgoingDegree());
                assertEquals(0, degrees.incomingDegree());
                assertRelationships(OUTGOING, node, type, traversal, newRelationship, existingRelationship);
            }
        }
    }

    @Test
    void shouldNewRelationshipBetweenAlreadyConnectedDenseNodes() throws Exception {
        long start;
        long end;
        long existingRelationship;
        int type, bulk;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            start = write.nodeCreate();
            end = write.nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            existingRelationship = write.relationshipCreate(start, type, end);
            bulk = tx.tokenWrite().relationshipTypeGetOrCreateForName("BULK");
            for (int i = 0; i < 100; i++) {
                write.relationshipCreate(start, bulk, write.nodeCreate());
            }
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            long newRelationship = write.relationshipCreate(start, type, write.nodeCreate());

            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor traversal =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                org.neo4j.internal.kernel.api.Read read = tx.dataRead();
                read.singleNode(start, node);
                assertTrue(node.next());
                assertTrue(node.supportsFastDegreeLookup());
                Degrees degrees = node.degrees(ALL_RELATIONSHIPS);
                for (int t : degrees.types()) {
                    if (t == type) {
                        assertEquals(2, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                        assertRelationships(OUTGOING, node, t, traversal, existingRelationship, newRelationship);

                    } else if (t == bulk) {
                        assertEquals(bulk, t);
                        assertEquals(100, degrees.outgoingDegree(t));
                        assertEquals(0, degrees.incomingDegree(t));
                    } else {
                        fail(t + "  is not the type you're looking for ");
                    }
                }
            }
        }
    }

    @Test
    void shouldCountNewRelationships() throws Exception {
        int relationship;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationship = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            write.relationshipCreate(write.nodeCreate(), relationship, write.nodeCreate());
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            write.relationshipCreate(write.nodeCreate(), relationship, write.nodeCreate());

            long countsTxState = tx.dataRead().countsForRelationship(-1, relationship, -1);

            assertEquals(2, countsTxState);
        }
    }

    @Test
    void shouldNotCountRemovedRelationships() throws Exception {
        int relationshipId;
        long relationship;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationshipId = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            relationship = write.relationshipCreate(write.nodeCreate(), relationshipId, write.nodeCreate());
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            write.relationshipDelete(relationship);

            long countsTxState = tx.dataRead().countsForRelationship(-1, relationshipId, -1);

            assertEquals(0, countsTxState);
        }
    }

    @Test
    void shouldCountNewRelationshipsRestrictedUser() throws Exception {
        int relationship;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationship = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            write.relationshipCreate(write.nodeCreate(), relationship, write.nodeCreate());
            tx.commit();
        }

        SecurityContext loginContext = new SecurityContext(
                AuthSubject.AUTH_DISABLED,
                new TestAccessMode(true, false, true, false, false),
                EMBEDDED_CONNECTION,
                null);
        try (KernelTransaction tx = beginTransaction(loginContext)) {
            Write write = tx.dataWrite();
            write.relationshipCreate(write.nodeCreate(), relationship, write.nodeCreate());

            long countsTxState = tx.dataRead().countsForRelationship(-1, relationship, -1);

            assertEquals(2, countsTxState);
        }
    }

    @Test
    void shouldNotCountRemovedRelationshipsRestrictedUser() throws Exception {
        int relationshipId;
        long relationship;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            relationshipId = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            relationship = write.relationshipCreate(write.nodeCreate(), relationshipId, write.nodeCreate());
            tx.commit();
        }

        SecurityContext loginContext = new SecurityContext(
                AuthSubject.AUTH_DISABLED,
                new TestAccessMode(true, false, true, false, false),
                EMBEDDED_CONNECTION,
                null);
        try (KernelTransaction tx = beginTransaction(loginContext)) {
            Write write = tx.dataWrite();
            write.relationshipDelete(relationship);

            long countsTxState = tx.dataRead().countsForRelationship(-1, relationshipId, -1);

            assertEquals(0, countsTxState);
        }
    }

    @Test
    void shouldIncludeAddedRelationshipsByTypeAndDirection() throws Exception {
        int typeId1;
        int typeId2;
        long relationship1;
        long relationship2;
        long sourceNode;
        long targetNode;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            typeId1 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            typeId2 = tx.tokenWrite().relationshipTypeGetOrCreateForName("R2");
            sourceNode = write.nodeCreate();
            relationship1 = write.relationshipCreate(sourceNode, typeId1, write.nodeCreate());
            relationship2 = write.relationshipCreate(sourceNode, typeId2, write.nodeCreate());
            targetNode = write.nodeCreate();
            tx.commit();
        }

        SecurityContext loginContext = new SecurityContext(
                AuthSubject.AUTH_DISABLED,
                new TestAccessMode(true, false, true, false, false),
                EMBEDDED_CONNECTION,
                null);
        try (KernelTransaction tx = beginTransaction(loginContext);
                NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                RelationshipTraversalCursor traversal =
                        tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            Write write = tx.dataWrite();
            long r1 = write.relationshipCreate(sourceNode, typeId1, targetNode); // OUTGOING :R
            long r2 = write.relationshipCreate(targetNode, typeId1, sourceNode); // INCOMING :R
            long r3 = write.relationshipCreate(sourceNode, typeId1, sourceNode); // LOOP :R
            long r4 = write.relationshipCreate(sourceNode, typeId2, targetNode); // OUTGOING :R2
            long r5 = write.relationshipCreate(targetNode, typeId2, sourceNode); // INCOMING :R2
            long r6 = write.relationshipCreate(sourceNode, typeId2, sourceNode); // LOOP :R2

            org.neo4j.internal.kernel.api.Read read = tx.dataRead();
            read.singleNode(sourceNode, node);
            assertTrue(node.next());

            assertRelationships(
                    node, traversal, ALL_RELATIONSHIPS, relationship1, relationship2, r1, r2, r3, r4, r5, r6);
            assertRelationships(node, traversal, selection(OUTGOING), relationship1, relationship2, r1, r3, r4, r6);
            assertRelationships(node, traversal, selection(typeId1, BOTH), relationship1, r1, r2, r3);
            assertRelationships(node, traversal, selection(typeId1, OUTGOING), relationship1, r1, r3);
            assertRelationships(node, traversal, selection(typeId1, INCOMING), r2, r3);
            assertRelationships(node, traversal, selection(typeId2, BOTH), relationship2, r4, r5, r6);
            assertRelationships(node, traversal, selection(typeId2, OUTGOING), relationship2, r4, r6);
            assertRelationships(node, traversal, selection(typeId2, INCOMING), r5, r6);
            assertRelationships(
                    node,
                    traversal,
                    selection(new int[] {typeId1, typeId2}, BOTH),
                    relationship1,
                    relationship2,
                    r1,
                    r2,
                    r3,
                    r4,
                    r5,
                    r6);
            assertRelationships(
                    node,
                    traversal,
                    selection(new int[] {typeId1, typeId2}, OUTGOING),
                    relationship1,
                    relationship2,
                    r1,
                    r3,
                    r4,
                    r6);
            assertRelationships(node, traversal, selection(new int[] {typeId1, typeId2}, INCOMING), r2, r3, r5, r6);
        }
    }

    private static void assertRelationships(
            Direction direction,
            NodeCursor node,
            int type,
            RelationshipTraversalCursor traversal,
            long... relationships) {
        assertRelationships(node, traversal, selection(type, direction), relationships);
    }

    private static void assertRelationships(
            NodeCursor node,
            RelationshipTraversalCursor traversal,
            RelationshipSelection selection,
            long... relationships) {
        node.relationships(traversal, selection);
        MutableLongSet set = LongHashSet.newSetWith(relationships);
        for (long relationship : relationships) {
            assertTrue(traversal.next());
            assertTrue(set.contains(traversal.relationshipReference()));
            set.remove(traversal.relationshipReference());
        }
        assertTrue(set.isEmpty());
        assertFalse(traversal.next());
    }

    private static void assertNoRelationships(
            Direction direction, NodeCursor node, int type, RelationshipTraversalCursor traversal) {
        node.relationships(traversal, selection(type, direction));
        assertFalse(traversal.next());
    }

    private void traverse(RelationshipTestSupport.StartNode start, boolean detached) throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            Map<String, Integer> expectedCounts = modifyStartNodeRelationships(start, tx);

            // given
            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT);
                    RelationshipTraversalCursor relationship =
                            tx.cursors().allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
                // when
                tx.dataRead().singleNode(start.id, node);

                assertTrue(node.next(), "access node");
                if (detached) {
                    tx.dataRead()
                            .relationships(start.id, node.relationshipsReference(), ALL_RELATIONSHIPS, relationship);
                } else {
                    node.relationships(relationship, ALL_RELATIONSHIPS);
                }

                Map<String, Integer> counts = count(tx, relationship);

                // then
                assertCounts(expectedCounts, counts);
            }

            tx.rollback();
        }
    }

    private static Map<String, Integer> modifyStartNodeRelationships(
            RelationshipTestSupport.StartNode start, KernelTransaction tx) throws KernelException {
        Map<String, Integer> expectedCounts = new HashMap<>();
        for (Map.Entry<String, List<RelationshipTestSupport.StartRelationship>> kv : start.relationships.entrySet()) {
            List<RelationshipTestSupport.StartRelationship> rs = kv.getValue();
            RelationshipTestSupport.StartRelationship head = rs.get(0);
            int type = tx.token().relationshipType(head.type.name());
            switch (head.direction) {
                case INCOMING:
                    tx.dataWrite().relationshipCreate(tx.dataWrite().nodeCreate(), type, start.id);
                    tx.dataWrite().relationshipCreate(tx.dataWrite().nodeCreate(), type, start.id);
                    break;
                case OUTGOING:
                    tx.dataWrite()
                            .relationshipCreate(start.id, type, tx.dataWrite().nodeCreate());
                    tx.dataWrite()
                            .relationshipCreate(start.id, type, tx.dataWrite().nodeCreate());
                    break;
                case BOTH:
                    tx.dataWrite().relationshipCreate(start.id, type, start.id);
                    tx.dataWrite().relationshipCreate(start.id, type, start.id);
                    break;
                default:
                    throw new IllegalStateException("Oh ye be cursed, foul checkstyle!");
            }
            tx.dataWrite().relationshipDelete(head.id);
            expectedCounts.put(kv.getKey(), rs.size() + 1);
        }

        String newTypeName = "NEW";
        int newType = tx.token().relationshipTypeGetOrCreateForName(newTypeName);
        tx.dataWrite().relationshipCreate(tx.dataWrite().nodeCreate(), newType, start.id);
        tx.dataWrite().relationshipCreate(start.id, newType, tx.dataWrite().nodeCreate());
        tx.dataWrite().relationshipCreate(start.id, newType, start.id);

        expectedCounts.put(computeKey(newTypeName, OUTGOING), 1);
        expectedCounts.put(computeKey(newTypeName, INCOMING), 1);
        expectedCounts.put(computeKey(newTypeName, BOTH), 1);

        return expectedCounts;
    }

    @Test
    void hasPropertiesShouldSeeNewlyCreatedProperties() throws Exception {
        // Given
        long relationship;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int token = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            relationship = write.relationshipCreate(write.nodeCreate(), token, write.nodeCreate());
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            try (RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                tx.dataRead().singleRelationship(relationship, cursor);
                assertTrue(cursor.next());
                assertFalse(hasProperties(cursor, tx));
                tx.dataWrite()
                        .relationshipSetProperty(
                                relationship,
                                tx.tokenWrite().propertyKeyGetOrCreateForName("prop"),
                                stringValue("foo"));
                assertTrue(hasProperties(cursor, tx));
            }
        }
    }

    @Test
    void hasPropertiesShouldSeeNewlyCreatedPropertiesOnNewlyCreatedRelationship() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int token = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            long relationship = write.relationshipCreate(write.nodeCreate(), token, write.nodeCreate());
            try (RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                tx.dataRead().singleRelationship(relationship, cursor);
                assertTrue(cursor.next());
                assertFalse(hasProperties(cursor, tx));
                tx.dataWrite()
                        .relationshipSetProperty(
                                relationship,
                                tx.tokenWrite().propertyKeyGetOrCreateForName("prop"),
                                stringValue("foo"));
                assertTrue(hasProperties(cursor, tx));
            }
        }
    }

    @Test
    void hasPropertiesShouldSeeNewlyRemovedProperties() throws Exception {
        // Given
        long relationship;
        int prop1, prop2, prop3;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int token = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            relationship = write.relationshipCreate(write.nodeCreate(), token, write.nodeCreate());
            prop1 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop1");
            prop2 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop2");
            prop3 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop3");
            tx.dataWrite().relationshipSetProperty(relationship, prop1, longValue(1));
            tx.dataWrite().relationshipSetProperty(relationship, prop2, longValue(2));
            tx.dataWrite().relationshipSetProperty(relationship, prop3, longValue(3));
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            try (RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT)) {
                tx.dataRead().singleRelationship(relationship, cursor);
                assertTrue(cursor.next());

                assertTrue(hasProperties(cursor, tx));
                tx.dataWrite().relationshipRemoveProperty(relationship, prop1);
                assertTrue(hasProperties(cursor, tx));
                tx.dataWrite().relationshipRemoveProperty(relationship, prop2);
                assertTrue(hasProperties(cursor, tx));
                tx.dataWrite().relationshipRemoveProperty(relationship, prop3);
                assertFalse(hasProperties(cursor, tx));
            }
        }
    }

    @Test
    void propertyTypeShouldBeTxStateAware() throws Exception {
        // Given
        long relationship;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            int token = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            relationship = write.relationshipCreate(write.nodeCreate(), token, write.nodeCreate());
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            try (RelationshipScanCursor relationships = tx.cursors().allocateRelationshipScanCursor(NULL_CONTEXT);
                    PropertyCursor properties = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
                tx.dataRead().singleRelationship(relationship, relationships);
                assertTrue(relationships.next());
                assertFalse(hasProperties(relationships, tx));
                int prop = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");
                tx.dataWrite().relationshipSetProperty(relationship, prop, stringValue("foo"));
                relationships.properties(properties);

                assertTrue(properties.next());
                assertThat(properties.propertyType()).isEqualTo(ValueGroup.TEXT);
            }
        }
    }

    private static boolean hasProperties(RelationshipScanCursor cursor, KernelTransaction tx) {
        try (PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            cursor.properties(propertyCursor);
            return propertyCursor.next();
        }
    }

    private static void relateNTimes(int nRelationshipsInStore, int type, long n1, long n2, KernelTransaction tx)
            throws KernelException {
        for (int i = 0; i < nRelationshipsInStore; i++) {
            tx.dataWrite().relationshipCreate(n1, type, n2);
        }
    }

    private static void assertCountRelationships(
            RelationshipScanCursor relationship, int expectedCount, long sourceNode, int type, long targetNode) {
        int count = 0;
        while (relationship.next()) {
            assertEquals(sourceNode, relationship.sourceNodeReference());
            assertEquals(type, relationship.type());
            assertEquals(targetNode, relationship.targetNodeReference());
            count++;
        }
        assertEquals(expectedCount, count);
    }

    private void assertCount(int count, RelationshipDirection direction, Consumer<Degrees> asserter) throws Exception {
        long start;
        int type;
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            start = write.nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName("R");
            for (int i = 0; i < count; i++) {
                createRelationship(direction, start, type, write);
            }
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            createRelationship(direction, start, type, write);
            try (NodeCursor node = tx.cursors().allocateNodeCursor(NULL_CONTEXT)) {
                Read read = tx.dataRead();
                read.singleNode(start, node);
                assertTrue(node.next());
                Degrees degrees = node.degrees(ALL_RELATIONSHIPS);
                asserter.accept(degrees);
            }
        }
    }

    private static void createRelationship(RelationshipDirection direction, long start, int type, Write write)
            throws EntityNotFoundException {
        switch (direction) {
            case OUTGOING:
                write.relationshipCreate(start, type, write.nodeCreate());
                break;
            case INCOMING:
                write.relationshipCreate(write.nodeCreate(), type, start);
                break;
            case LOOP:
                write.relationshipCreate(start, type, start);
                break;
            default:
                throw new IllegalStateException("Checkstyle, you win again!");
        }
    }
}
