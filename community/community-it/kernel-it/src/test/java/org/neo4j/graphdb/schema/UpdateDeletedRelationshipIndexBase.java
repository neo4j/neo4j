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
package org.neo4j.graphdb.schema;

import static org.neo4j.test.Race.throwing;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
abstract class UpdateDeletedRelationshipIndexBase {
    @Inject
    protected GraphDatabaseAPI db;

    protected static final String KEY = "key";
    protected static final RelationshipType TYPE_ONE = RelationshipType.withName("TypeOne");

    private static final int RELATIONSHIPS = 100;

    @RepeatedTest(5)
    void shouldHandleCreateRelationshipConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation((tx, id) ->
                tx.createNode().createRelationshipTo(tx.createNode(), TYPE_ONE).setProperty(KEY, id));
    }

    @RepeatedTest(5)
    void shouldHandleDeleteRelationshipConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation(
                (tx, id) -> tx.getRelationshipById(id).delete());
    }

    @RepeatedTest(5)
    void shouldHandleRemovePropertyConcurrentlyWithIndexDrop() throws Throwable {
        shouldHandleIndexDropConcurrentlyWithOperation(
                (tx, id) -> tx.getRelationshipById(id).removeProperty(KEY));
    }

    private void shouldHandleIndexDropConcurrentlyWithOperation(RelationshipOperation operation) throws Throwable {
        // given
        long[] relationships = createRelationships();
        IndexDefinition indexDefinition = createIndex();

        // when
        Race race = new Race();
        race.addContestant(
                () -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema().getIndexByName(indexDefinition.getName()).drop();
                        tx.commit();
                    }
                },
                1);
        for (int i = 0; i < RELATIONSHIPS; i++) {
            final long id = relationships[i];
            race.addContestant(throwing(() -> {
                try (Transaction tx = db.beginTx()) {
                    operation.run(tx, id);
                    tx.commit();
                } catch (TransientTransactionFailureException e) {
                    // OK
                }
            }));
        }

        // then
        race.go();
    }

    private long[] createRelationships() {
        long[] result = new long[RELATIONSHIPS];
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < RELATIONSHIPS; i++) {
                var rel = tx.createNode().createRelationshipTo(tx.createNode(), TYPE_ONE);
                rel.setProperty(KEY, i);
                result[i] = rel.getId();
            }
            tx.commit();
        }
        return result;
    }

    private IndexDefinition createIndex() {
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < RELATIONSHIPS; i++) {
                tx.createNode().createRelationshipTo(tx.createNode(), TYPE_ONE).setProperty(KEY, i);
            }
            tx.commit();
        }
        IndexDefinition indexDefinition;
        indexDefinition = indexCreate();
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
        return indexDefinition;
    }

    protected abstract IndexDefinition indexCreate();

    private interface RelationshipOperation {
        void run(Transaction tx, long relId) throws Exception;
    }
}
