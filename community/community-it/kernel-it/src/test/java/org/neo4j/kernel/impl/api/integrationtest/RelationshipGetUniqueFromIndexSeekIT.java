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
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.locking.forseti.ForsetiClient;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class RelationshipGetUniqueFromIndexSeekIT extends KernelIntegrationTest {
    private int relationshipTypeId;
    private int propertyId1;
    private int propertyId2;

    @BeforeEach
    void createKeys() throws Exception {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        this.relationshipTypeId = tokenWrite.relationshipTypeGetOrCreateForName("R");
        this.propertyId1 = tokenWrite.propertyKeyGetOrCreateForName("foo");
        this.propertyId2 = tokenWrite.propertyKeyGetOrCreateForName("bar");
        commit();
    }

    // relationshipGetUniqueWithLabelAndProperty(statement, :Person, foo=val)
    //
    // Given we have a unique constraint on :R(foo)
    // (If not, throw)
    //
    // If there is a relationship r with r:R and r.foo == val, return it
    // If there is no such relationship, return ?
    //
    // Ensure that if that method is called again with the same argument from some other transaction,
    // that transaction blocks until this transaction has finished
    //

    // [X] must return relationship from the unique index with the given property
    // [X] must return NO_SUCH_RELATIONSHIP if it is not in the index for the given property
    //
    // must block other transactions that try to call it with the same arguments

    @Test
    void shouldFindMatchingRelationship() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(relationshipTypeId, propertyId1);
        Value value = Values.of("value");
        long relId = createRelationshipWithValue(value);

        // when looking for it
        KernelTransaction transaction = newTransaction();
        Read read = transaction.dataRead();
        int propertyId = index.schema().getPropertyIds()[0];
        try (RelationshipValueIndexCursor cursor = transaction
                .cursors()
                .allocateRelationshipValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId = read.lockingRelationshipUniqueIndexSeek(index, cursor, exact(propertyId, value));

            // then
            assertEquals(relId, foundId, "Created relationship was not found");
        }
        commit();
    }

    @Test
    void shouldNotFindNonMatchingRelationship() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(relationshipTypeId, propertyId1);
        Value value = Values.of("value");
        createRelationshipWithValue(Values.of("other_" + value));

        // when looking for it
        KernelTransaction transaction = newTransaction();
        try (RelationshipValueIndexCursor cursor = transaction
                .cursors()
                .allocateRelationshipValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId =
                    transaction.dataRead().lockingRelationshipUniqueIndexSeek(index, cursor, exact(propertyId1, value));

            // then
            assertTrue(isNoSuchRelationship(foundId), "Non-matching created relationship was found");
        }
        commit();
    }

    @Test
    void shouldCompositeFindMatchingRelationship() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(relationshipTypeId, propertyId1, propertyId2);
        Value value1 = Values.of("value1");
        Value value2 = Values.of("value2");
        long relId = createRelationshipWithValues(value1, value2);

        // when looking for it
        KernelTransaction transaction = newTransaction();
        try (RelationshipValueIndexCursor cursor = transaction
                .cursors()
                .allocateRelationshipValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId = transaction
                    .dataRead()
                    .lockingRelationshipUniqueIndexSeek(
                            index, cursor, exact(propertyId1, value1), exact(propertyId2, value2));

            // then
            assertEquals(relId, foundId, "Created relationship was not found");
        }
        commit();
    }

    @Test
    void shouldNotCompositeFindNonMatchingRelationship() throws Exception {
        // given
        IndexDescriptor index = createUniquenessConstraint(relationshipTypeId, propertyId1, propertyId2);
        Value value1 = Values.of("value1");
        Value value2 = Values.of("value2");
        createRelationshipWithValues(Values.of("other_" + value1), Values.of("other_" + value2));

        // when looking for it
        KernelTransaction transaction = newTransaction();
        try (RelationshipValueIndexCursor cursor = transaction
                .cursors()
                .allocateRelationshipValueIndexCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            long foundId = transaction
                    .dataRead()
                    .lockingRelationshipUniqueIndexSeek(
                            index, cursor, exact(propertyId1, value1), exact(propertyId2, value2));

            // then
            assertTrue(isNoSuchRelationship(foundId), "Non-matching created relationship was found");
        }
        commit();
    }

    @Test
    void shouldBlockUniqueIndexSeekFromCompetingTransaction() throws Exception {
        // This is the interleaving that we are trying to verify works correctly:
        // ----------------------------------------------------------------------
        // Thread1 (main)        : Thread2
        // create unique rel     :
        // lookup(rel)           :
        // open start latch ----->
        //    |                  : lookup(rel)
        // wait for T2 to block  :      |
        //                       :    *block*
        // commit --------------->   *unblock*
        // wait for T2 end latch :      |
        //                       : finish transaction
        //                       : open end latch
        // *unblock* <-------------‘
        // assert that we complete before timeout
        final var latch = new BinaryLatch();

        IndexDescriptor index = createUniquenessConstraint(relationshipTypeId, propertyId1);
        Value value = Values.of("value");

        Write write = dataWriteInNewTransaction();
        long relId = write.relationshipCreate(write.nodeCreate(), relationshipTypeId, write.nodeCreate());

        // This adds the relationship to the unique index and should take an index write lock
        write.relationshipSetProperty(relId, propertyId1, value);

        try (OtherThreadExecutor other = new OtherThreadExecutor("Transaction Thread 2")) {
            var future = other.executeDontWait(() -> {
                latch.await();
                try (KernelTransaction tx =
                        kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
                    try (RelationshipValueIndexCursor cursor =
                            tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                        tx.dataRead().lockingRelationshipUniqueIndexSeek(index, cursor, exact(propertyId1, value));
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
    void shouldMakeSureWeBlockOtherThreadsFromCreatingRelationship() throws Throwable {
        // This simulates a MERGE ()-[r:R {foo: "value"}]->() query. Here we do that by having multiple threads
        // first trying to find the relationship and if not there it will create the relationship. No matter the number
        // of threads we should always have one thread creating the node and the rest of them should find it.
        IndexDescriptor index = createUniquenessConstraint(relationshipTypeId, propertyId1);
        Value value = Values.of("value");
        Race race = new Race();
        AtomicInteger readCounter = new AtomicInteger();
        AtomicInteger writeCounter = new AtomicInteger();
        int contestants = Runtime.getRuntime().availableProcessors();
        race.addContestants(contestants, Race.throwing(() -> {
            try (KernelTransaction tx =
                    kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED)) {
                try (RelationshipValueIndexCursor cursor =
                        tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                    long relId =
                            tx.dataRead().lockingRelationshipUniqueIndexSeek(index, cursor, exact(propertyId1, value));
                    if (relId == StatementConstants.NO_SUCH_RELATIONSHIP) {
                        Write write = tx.dataWrite();
                        relId = write.relationshipCreate(write.nodeCreate(), relationshipTypeId, write.nodeCreate());
                        write.relationshipSetProperty(relId, propertyId1, value);
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

    private static boolean isNoSuchRelationship(long foundId) {
        return StatementConstants.NO_SUCH_RELATIONSHIP == foundId;
    }

    private long createRelationshipWithValue(Value value) throws KernelException {
        Write write = dataWriteInNewTransaction();
        long relId = write.relationshipCreate(write.nodeCreate(), relationshipTypeId, write.nodeCreate());
        write.relationshipSetProperty(relId, propertyId1, value);
        commit();
        return relId;
    }

    private long createRelationshipWithValues(Value value1, Value value2) throws KernelException {
        Write write = dataWriteInNewTransaction();
        long relId = write.relationshipCreate(write.nodeCreate(), relationshipTypeId, write.nodeCreate());
        write.relationshipSetProperty(relId, propertyId1, value1);
        write.relationshipSetProperty(relId, propertyId2, value2);
        commit();
        return relId;
    }

    private IndexDescriptor createUniquenessConstraint(int typeId, int... propertyIds) throws Exception {
        KernelTransaction transaction = newTransaction(LoginContext.AUTH_DISABLED);
        RelationTypeSchemaDescriptor schema = SchemaDescriptors.forRelType(typeId, propertyIds);
        ConstraintDescriptor constraint =
                transaction.schemaWrite().uniquePropertyConstraintCreate(IndexPrototype.uniqueForSchema(schema));
        IndexDescriptor index = transaction.schemaRead().indexGetForName(constraint.getName());
        commit();
        return index;
    }
}
