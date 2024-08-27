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
package org.neo4j.consistency.checking;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.RecordType;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
public class DetectAllRelationshipInconsistenciesIT {
    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private DatabaseManagementService managementService;

    @Test
    void shouldDetectSabotagedRelationshipWhereEverItIs() throws Exception {
        // GIVEN a database which lots of relationships
        GraphDatabaseAPI db = getGraphDatabaseAPI();
        Sabotage sabotage;
        try {
            Node[] nodes = new Node[1_000];
            Relationship[] relationships = new Relationship[10_000];
            long additionalNodeId;
            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i] = tx.createNode(label("Foo"));
                }
                additionalNodeId = tx.createNode().getId();
                for (int i = 0; i < 10_000; i++) {
                    relationships[i] = random.among(nodes).createRelationshipTo(random.among(nodes), MyRelTypes.TEST);
                }
                tx.commit();
            }

            // WHEN sabotaging a random relationship
            DependencyResolver resolver = db.getDependencyResolver();
            RecordStorageEngine recordStorageEngine = resolver.resolveDependency(RecordStorageEngine.class);
            NeoStores neoStores = recordStorageEngine.testAccessNeoStores();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            Relationship sabotagedRelationships = random.among(relationships);
            try (var storeCursors = recordStorageEngine.createStorageCursors(NULL_CONTEXT)) {
                sabotage = sabotage(relationshipStore, sabotagedRelationships.getId(), additionalNodeId, storeCursors);
            }
        } finally {
            managementService.shutdown();
        }

        // THEN the checker should find it, where ever it is in the store
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        ConsistencyCheckService.Result result = new ConsistencyCheckService(
                        Neo4jLayout.of(directory.homePath()).databaseLayout(DEFAULT_DATABASE_NAME))
                .with(getTuningConfiguration())
                .with(logProvider)
                .runFullConsistencyCheck();
        assertThat(result.isSuccessful()).isFalse();
        LogAssertions.assertThat(logProvider).containsMessages(sabotage.after.toString());

        int relationshipInconsistencies =
                result.summary().getInconsistencyCountForRecordType(RecordType.RELATIONSHIP.name());

        assertTrue(relationshipInconsistencies > 0, "Couldn't detect sabotaged relationship " + sabotage);
    }

    private Config getTuningConfiguration() {
        return Config.newBuilder()
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .set(getConfig())
                .build();
    }

    private GraphDatabaseAPI getGraphDatabaseAPI() {
        managementService = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setConfig(getConfig())
                .build();
        GraphDatabaseService database = managementService.database(DEFAULT_DATABASE_NAME);
        return (GraphDatabaseAPI) database;
    }

    protected Map<Setting<?>, Object> getConfig() {
        return Map.of(GraphDatabaseSettings.db_format, PageAligned.LATEST_NAME);
    }

    private static class Sabotage {
        private final RelationshipRecord before;
        private final RelationshipRecord after;
        private final RelationshipRecord other;

        Sabotage(RelationshipRecord before, RelationshipRecord after, RelationshipRecord other) {
            this.before = before;
            this.after = after;
            this.other = other;
        }

        @Override
        public String toString() {
            return "Sabotaged " + before + " --> " + after + ", other relationship " + other;
        }
    }

    private Sabotage sabotage(RelationshipStore store, long id, long lonelyNodeId, StoreCursors storeCursors) {
        RelationshipRecord before = store.newRecord();
        try (var cursor = store.openPageCursorForReading(id, NULL_CONTEXT)) {
            store.getRecordByCursor(id, before, RecordLoad.NORMAL, cursor, EmptyMemoryTracker.INSTANCE);
        }
        RelationshipRecord after = new RelationshipRecord(before);

        boolean sabotageSourceChain = random.nextBoolean(); // otherwise target chain
        boolean sabotageNodeId = random.nextBoolean();
        long otherReference = NULL_REFERENCE.longValue();
        if (sabotageNodeId) {
            boolean useLonelyNodeId = random.nextBoolean();
            if (sabotageSourceChain) {
                after.setFirstNode(useLonelyNodeId ? lonelyNodeId : after.getFirstNode() + 1);
            } else {
                after.setSecondNode(useLonelyNodeId ? lonelyNodeId : after.getSecondNode() + 1);
            }
        } else {
            if (sabotageSourceChain) {
                if (!after.isFirstInFirstChain()) {
                    after.setFirstPrevRel(otherReference = after.getFirstPrevRel() + 1);
                } else {
                    after.setFirstNextRel(otherReference = after.getFirstNextRel() + 1);
                }
            } else {
                if (!after.isFirstInSecondChain()) {
                    after.setSecondPrevRel(otherReference = after.getSecondPrevRel() + 1);
                } else {
                    after.setSecondNextRel(otherReference = after.getSecondNextRel() + 1);
                }
            }
        }

        store.prepareForCommit(after, store.getIdGenerator(), NULL_CONTEXT);
        try (var storeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            store.updateRecord(after, storeCursor, NULL_CONTEXT, storeCursors);
        }

        RelationshipRecord other = NULL_REFERENCE.is(otherReference) ? null : loadRecord(store, otherReference);
        return new Sabotage(before, after, other);
    }

    private RelationshipRecord loadRecord(RelationshipStore store, long otherReference) {
        try (var cursor = store.openPageCursorForReading(otherReference, NULL_CONTEXT)) {
            return store.getRecordByCursor(
                    otherReference, store.newRecord(), RecordLoad.FORCE, cursor, EmptyMemoryTracker.INSTANCE);
        }
    }
}
