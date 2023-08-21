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
package org.neo4j.kernel.impl.api.integrationtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.locking.forseti.ForsetiClient;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class NodeGetUniqueFromIndexSeekIT extends KernelIntegrationTest {
    private int labelId;
    private int propertyId1;
    private int propertyId2;

    @BeforeEach
    void createKeys() throws Exception {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        this.labelId = tokenWrite.labelGetOrCreateForName("Person");
        this.propertyId1 = tokenWrite.propertyKeyGetOrCreateForName("foo");
        this.propertyId2 = tokenWrite.propertyKeyGetOrCreateForName("bar");
        commit();
    }

    // nodeGetUniqueWithLabelAndProperty(statement, :Person, foo=val)
    //
    // Given we have a unique constraint on :Person(foo)
    // (If not, throw)
    //
    // If there is a node n with n:Person and n.foo == val, return it
    // If there is no such node, return ?
    //
    // Ensure that if that method is called again with the same argument from some other transaction,
    // that transaction blocks until this transaction has finished
    //

    // [X] must return node from the unique index with the given property
    // [X] must return NO_SUCH_NODE if it is not in the index for the given property
    //
    // must block other transactions that try to call it with the same arguments

    @Test
    void shouldFindMatchingNode() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(labelId, propertyId1);
        Value value = Values.of("value");
        long nodeId = createNodeWithValue(value);

        // when looking for it
        KernelTransaction transaction = newTransaction();
        Read read = transaction.dataRead();
        int propertyId = index.schema().getPropertyIds()[0];
        try (NodeValueIndexCursor cursor = transaction
                .cursors()
                .allocateNodeValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId = read.lockingNodeUniqueIndexSeek(index, cursor, exact(propertyId, value));

            // then
            assertEquals(nodeId, foundId, "Created node was not found");
        }
        commit();
    }

    @Test
    void shouldNotFindNonMatchingNode() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(labelId, propertyId1);
        Value value = Values.of("value");
        createNodeWithValue(Values.of("other_" + value));

        // when looking for it
        KernelTransaction transaction = newTransaction();
        try (NodeValueIndexCursor cursor = transaction
                .cursors()
                .allocateNodeValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId = transaction.dataRead().lockingNodeUniqueIndexSeek(index, cursor, exact(propertyId1, value));

            // then
            assertTrue(isNoSuchNode(foundId), "Non-matching created node was found");
        }
        commit();
    }

    @Test
    void shouldCompositeFindMatchingNode() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(labelId, propertyId1, propertyId2);
        Value value1 = Values.of("value1");
        Value value2 = Values.of("value2");
        long nodeId = createNodeWithValues(value1, value2);

        // when looking for it
        KernelTransaction transaction = newTransaction();
        try (NodeValueIndexCursor cursor = transaction
                .cursors()
                .allocateNodeValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId = transaction
                    .dataRead()
                    .lockingNodeUniqueIndexSeek(index, cursor, exact(propertyId1, value1), exact(propertyId2, value2));

            // then
            assertEquals(nodeId, foundId, "Created node was not found");
        }
        commit();
    }

    @Test
    void shouldNotCompositeFindNonMatchingNode() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(labelId, propertyId1, propertyId2);
        Value value1 = Values.of("value1");
        Value value2 = Values.of("value2");
        createNodeWithValues(Values.of("other_" + value1), Values.of("other_" + value2));

        // when looking for it
        KernelTransaction transaction = newTransaction();
        try (NodeValueIndexCursor cursor = transaction
                .cursors()
                .allocateNodeValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId = transaction
                    .dataRead()
                    .lockingNodeUniqueIndexSeek(index, cursor, exact(propertyId1, value1), exact(propertyId2, value2));

            // then
            assertTrue(isNoSuchNode(foundId), "Non-matching created node was found");
        }
        commit();
    }

    @Test // ( timeout = 10_000 )
    void shouldBlockUniqueIndexSeekFromCompetingTransaction() throws Exception {
        // This is the interleaving that we are trying to verify works correctly:
        // ----------------------------------------------------------------------
        // Thread1 (main)        : Thread2
        // create unique node    :
        // lookup(node)          :
        // open start latch ----->
        //    |                  : lookup(node)
        // wait for T2 to block  :      |
        //                       :    *block*
        // commit --------------->   *unblock*
        // wait for T2 end latch :      |
        //                       : finish transaction
        //                       : open end latch
        // *unblock* <-------------‘
        // assert that we complete before timeout
        final var latch = new BinaryLatch();

        IndexDescriptor index = createUniquenessConstraint(labelId, propertyId1);
        Value value = Values.of("value");

        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel(nodeId, labelId);

        // This adds the node to the unique index and should take an index write lock
        write.nodeSetProperty(nodeId, propertyId1, value);

        try (OtherThreadExecutor other = new OtherThreadExecutor("Transaction Thread 2")) {
            var future = other.executeDontWait(() -> {
                latch.await();
                try (KernelTransaction tx =
                        kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
                    try (NodeValueIndexCursor cursor =
                            tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                        tx.dataRead().lockingNodeUniqueIndexSeek(index, cursor, exact(propertyId1, value));
                    }
                    tx.commit();
                }
                return null;
            });

            latch.release();
            other.waitUntilWaiting(details -> details.isAt(ForsetiClient.class, "acquireShared"));

            commit();
            future.get();
        }
    }

    @Test
    void shouldMakeSureWeBlockOtherThreadsFromCreatingNode() throws Throwable {
        // This simulates a MERGE (n:Person {foo: "value"}) query. Here we do that by having multiple threads
        // first trying to find the node and if not there it will create the node. No matter the number
        // of threads we should always have one thread creating the node and the rest of them should find it.
        IndexDescriptor index = createUniquenessConstraint(labelId, propertyId1);
        Value value = Values.of("value");
        Race race = new Race();
        AtomicInteger readCounter = new AtomicInteger();
        AtomicInteger writeCounter = new AtomicInteger();
        int contestants = Runtime.getRuntime().availableProcessors();
        race.addContestants(contestants, Race.throwing(() -> {
            try (KernelTransaction tx =
                    kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
                try (NodeValueIndexCursor cursor =
                        tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                    long nodeId = tx.dataRead().lockingNodeUniqueIndexSeek(index, cursor, exact(propertyId1, value));
                    if (nodeId == StatementConstants.NO_SUCH_NODE) {
                        Write write = tx.dataWrite();
                        nodeId = write.nodeCreate();
                        write.nodeAddLabel(nodeId, labelId);
                        write.nodeSetProperty(nodeId, propertyId1, value);
                        writeCounter.incrementAndGet();
                    } else {
                        readCounter.incrementAndGet();
                    }
                }
                tx.commit();
            }
        }));
        race.go();
        assertEquals(contestants - 1, readCounter.get());
        assertEquals(1, writeCounter.get());
    }

    private static boolean isNoSuchNode(long foundId) {
        return StatementConstants.NO_SUCH_NODE == foundId;
    }

    private long createNodeWithValue(Value value) throws KernelException {
        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel(nodeId, labelId);
        write.nodeSetProperty(nodeId, propertyId1, value);
        commit();
        return nodeId;
    }

    private long createNodeWithValues(Value value1, Value value2) throws KernelException {
        Write write = dataWriteInNewTransaction();
        long nodeId = write.nodeCreate();
        write.nodeAddLabel(nodeId, labelId);
        write.nodeSetProperty(nodeId, propertyId1, value1);
        write.nodeSetProperty(nodeId, propertyId2, value2);
        commit();
        return nodeId;
    }

    private IndexDescriptor createUniquenessConstraint(int labelId, int... propertyIds) throws Exception {
        KernelTransaction transaction = newTransaction(LoginContext.AUTH_DISABLED);
        LabelSchemaDescriptor schema = SchemaDescriptors.forLabel(labelId, propertyIds);
        ConstraintDescriptor constraint =
                transaction.schemaWrite().uniquePropertyConstraintCreate(IndexPrototype.uniqueForSchema(schema));
        IndexDescriptor index = transaction.schemaRead().indexGetForName(constraint.getName());
        commit();
        return index;
    }
}
