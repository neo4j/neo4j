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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch;
import static org.neo4j.io.IOUtils.uncheckedConsumer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.AnyTokenSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@DbmsExtension
@ExtendWith(RandomExtension.class)
class RelationshipTypeIndexIT {
    private static final RelationshipType REL_TYPE = RelationshipType.withName("REL_TYPE");
    private static final RelationshipType OTHER_REL_TYPE = RelationshipType.withName("OTHER_REL_TYPE");
    private static final String PROPERTY = "prop";
    private static final String PROPERTY_VALUE = "value";

    @Inject
    GraphDatabaseService db;

    @Inject
    DbmsController dbmsController;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    RandomSupport random;

    @Test
    void shouldSeeAddedRelationship() throws IndexNotFoundKernelException {
        Set<String> expectedIds = new HashSet<>();
        createRelationshipInTx(expectedIds);

        assertContainsRelationships(expectedIds);
    }

    @Test
    void shouldNotSeeRemovedRelationship() throws IndexNotFoundKernelException {
        Set<String> expectedIds = new HashSet<>();
        String nodeId;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(TestLabels.LABEL_ONE);
            nodeId = node.getElementId();
            node.createRelationshipTo(tx.createNode(), REL_TYPE);
            expectedIds.add(createRelationship(tx).getElementId());
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeByElementId(nodeId);
            Iterables.forEach(node.getRelationships(), Relationship::delete);
            tx.commit();
        }

        assertContainsRelationships(expectedIds);
    }

    @Test
    void shouldRebuildIfMissingDuringStartup() throws IndexNotFoundKernelException, IOException {
        Set<String> expectedIds = new HashSet<>();
        createRelationshipInTx(expectedIds);

        ResourceIterator<Path> files = getRelationshipTypeIndexFiles();
        dbmsController.restartDbms(builder -> {
            files.forEachRemaining(uncheckedConsumer(file -> fs.deleteFile(file)));
            return builder;
        });
        awaitIndexesOnline();

        assertContainsRelationships(expectedIds);
    }

    @Test
    void shouldPopulateIndex() throws KernelException {
        int numberOfRelationships = 10;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < numberOfRelationships; i++) {
                createRelationship(tx);
            }
            tx.commit();
        }

        String indexName = createFulltextRelationshipIndex();

        assertEquals(numberOfRelationships, countRelationshipsInFulltextIndex(indexName));
    }

    @Test
    void shouldBeRecovered() throws IndexNotFoundKernelException {
        Set<String> expectedIds = new HashSet<>();
        createRelationshipInTx(expectedIds);

        dbmsController.restartDbms(builder -> {
            removeLastCheckpointRecordFromLastLogFile();
            return builder;
        });
        awaitIndexesOnline();

        assertContainsRelationships(expectedIds);
    }

    @Test
    void shouldRecoverIndex() throws KernelException {
        int numberOfRelationships = 10;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < numberOfRelationships; i++) {
                createRelationship(tx);
            }
            tx.commit();
        }

        String indexName = createFulltextRelationshipIndex();

        dbmsController.restartDbms(builder -> {
            removeLastCheckpointRecordFromLastLogFile();
            return builder;
        });
        awaitIndexesOnline();

        assertEquals(numberOfRelationships, countRelationshipsInFulltextIndex(indexName));
    }

    @Test
    void shouldCorrectlyValidateRelationshipPropertyExistenceConstraint() {
        // A single random relationship that violates constraint
        // together with a set of relevant and irrelevant relationships.
        try (Transaction tx = db.beginTx()) {
            int invalidRelationship = random.nextInt(100);
            for (int i = 0; i < 100; i++) {
                if (i == invalidRelationship) {
                    // This relationship doesn't have the demanded property
                    tx.createNode().createRelationshipTo(tx.createNode(), REL_TYPE);
                } else {
                    createRelationship(tx);
                    createRelationship(tx, OTHER_REL_TYPE);
                }
            }
            tx.commit();
        }

        assertThrows(ConstraintViolationException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                tx.schema()
                        .constraintFor(REL_TYPE)
                        .assertPropertyExists(PROPERTY)
                        .create();
                tx.commit();
            }
        });
    }

    private ResourceIterator<Path> getRelationshipTypeIndexFiles() throws IndexNotFoundKernelException, IOException {
        return getIndexProxy().snapshotFiles();
    }

    private static Relationship createRelationship(Transaction tx) {
        return createRelationship(tx, REL_TYPE);
    }

    private static Relationship createRelationship(Transaction tx, RelationshipType type) {
        Relationship relationship = tx.createNode().createRelationshipTo(tx.createNode(), type);
        relationship.setProperty(PROPERTY, PROPERTY_VALUE);
        return relationship;
    }

    private void createRelationshipInTx(Set<String> expectedIds) {
        try (Transaction tx = db.beginTx()) {
            Relationship relationship = createRelationship(tx);
            expectedIds.add(relationship.getElementId());
            tx.commit();
        }
    }

    IndexDescriptor findTokenIndex() {
        try (Transaction tx = db.beginTx()) {
            for (IndexDefinition indexDef : tx.schema().getIndexes()) {
                IndexDescriptor index = ((IndexDefinitionImpl) indexDef).getIndexReference();
                if (index.schema().isSchemaDescriptorType(AnyTokenSchemaDescriptor.class)
                        && index.schema().entityType() == EntityType.RELATIONSHIP
                        && index.getIndexType() == org.neo4j.internal.schema.IndexType.LOOKUP) {
                    return index;
                }
            }
        }
        fail("Didn't find expected token index");
        return null;
    }

    private IndexProxy getIndexProxy() throws IndexNotFoundKernelException {
        IndexingService indexingService =
                ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(IndexingService.class);
        return indexingService.getIndexProxy(findTokenIndex());
    }

    private void assertContainsRelationships(Set<String> expectedIds) throws IndexNotFoundKernelException {
        assertThat(getIndexProxy().getState()).isEqualTo(InternalIndexState.ONLINE);
        try (var tx = db.beginTx()) {
            try (var relationships = tx.findRelationships(REL_TYPE)) {
                var actualIds = new HashSet<String>();
                while (relationships.hasNext()) {
                    var relationship = relationships.next();
                    actualIds.add(relationship.getElementId());
                }
                assertThat(actualIds).as("contains expected relationships").isEqualTo(expectedIds);
            }
        }
    }

    private int countRelationshipsInFulltextIndex(String indexName) throws KernelException {
        int relationshipsInIndex;
        try (Transaction transaction = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) transaction).kernelTransaction();
            IndexDescriptor index = ktx.schemaRead().indexGetForName(indexName);
            IndexReadSession indexReadSession = ktx.dataRead().indexReadSession(index);
            relationshipsInIndex = 0;
            try (RelationshipValueIndexCursor cursor =
                    ktx.cursors().allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                ktx.dataRead()
                        .relationshipIndexSeek(
                                ktx.queryContext(), indexReadSession, cursor, unconstrained(), fulltextSearch("*"));
                while (cursor.next()) {
                    relationshipsInIndex++;
                }
            }
        }
        return relationshipsInIndex;
    }

    private String createFulltextRelationshipIndex() {
        String indexName;
        try (Transaction transaction = db.beginTx()) {
            indexName = transaction
                    .schema()
                    .indexFor(REL_TYPE)
                    .on(PROPERTY)
                    .withIndexType(IndexType.FULLTEXT)
                    .create()
                    .getName();
            transaction.commit();
        }
        awaitIndexesOnline();
        return indexName;
    }

    private void awaitIndexesOnline() {
        try (Transaction transaction = db.beginTx()) {
            transaction.schema().awaitIndexesOnline(10, TimeUnit.MINUTES);
            transaction.commit();
        }
    }

    private void removeLastCheckpointRecordFromLastLogFile() {
        try {
            LogFiles logFiles = buildLogFiles();
            Optional<CheckpointInfo> latestCheckpoint =
                    logFiles.getCheckpointFile().findLatestCheckpoint();
            if (latestCheckpoint.isPresent()) {
                try (StoreChannel storeChannel =
                        fs.write(logFiles.getCheckpointFile().getCurrentFile())) {
                    storeChannel.truncate(
                            latestCheckpoint.get().checkpointEntryPosition().getByteOffset());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private LogFiles buildLogFiles() throws IOException {
        final var databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
        return LogFilesBuilder.logFilesBasedOnlyBuilder(databaseLayout.getTransactionLogsDirectory(), fs)
                .build();
    }
}
