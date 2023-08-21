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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.io.fs.FileUtils.copyDirectory;
import static org.neo4j.test.TestLabels.LABEL_ONE;
import static org.neo4j.test.TestLabels.LABEL_THREE;
import static org.neo4j.test.TestLabels.LABEL_TWO;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@DbmsExtension(configurationCallback = "configure")
@ExtendWith(RandomExtension.class)
class IndexConsistencyIT {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private IndexProviderMap indexProviderMap;

    @Inject
    private RandomSupport random;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, PageAligned.LATEST_NAME);
    }

    private final AssertableLogProvider log = new AssertableLogProvider();
    private static final Label[] LABELS = new Label[] {LABEL_ONE, LABEL_TWO, LABEL_THREE};
    private static final RelationshipType TYPE_ONE = RelationshipType.withName("TYPE_ONE");
    private static final RelationshipType TYPE_TWO = RelationshipType.withName("TYPE_TWO");
    private static final RelationshipType[] TYPES = new RelationshipType[] {TYPE_ONE, TYPE_TWO};
    private static final String PROPERTY_KEY = "numericProperty";
    private static final String PROPERTY_KEY2 = "property2";
    private static final String[] PROPERTY_KEYS = new String[] {PROPERTY_KEY, PROPERTY_KEY2};
    private static final double DELETE_RATIO = 0.2;
    private static final double UPDATE_RATIO = 0.2;
    private static final int ENTITY_COUNT_BASELINE = 10;
    private final Predicate<Path> SOURCE_COPY_FILE_FILTER =
            path -> Files.isDirectory(path) || path.getFileName().toString().startsWith("index");

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void reportNotCleanNativeIndex(EntityType entityType) throws IOException, ConsistencyCheckIncompleteException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        someData(entityType);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("forcedCheckpoint"));
        Path indexesCopy = databaseLayout.file("indexesCopy");
        Path indexSources =
                indexProviderMap.getDefaultProvider().directoryStructure().rootDirectory();
        copyDirectory(indexSources, indexesCopy, SOURCE_COPY_FILE_FILTER);

        try (Transaction tx = db.beginTx()) {
            createNewEntity(tx, entityType);
            tx.commit();
        }

        managementService.shutdown();

        copyDirectory(indexesCopy, indexSources);

        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse(result.isSuccessful(), "Expected consistency check to fail");
        assertThat(readReport(result))
                .contains(
                        "WARN  Index was dirty on startup which means it was not shutdown correctly and need to be cleaned up with a successful recovery.");
    }

    @ParameterizedTest
    @EnumSource(EntityType.class)
    void reportNotCleanNativeIndexWithCorrectData(EntityType entityType)
            throws IOException, ConsistencyCheckIncompleteException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        someData(entityType);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("forcedCheckpoint"));
        Path indexesCopy = databaseLayout.file("indexesCopy");
        Path indexSources =
                indexProviderMap.getDefaultProvider().directoryStructure().rootDirectory();
        copyDirectory(indexSources, indexesCopy, SOURCE_COPY_FILE_FILTER);

        managementService.shutdown();

        copyDirectory(indexesCopy, indexSources);

        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertTrue(result.isSuccessful(), "Expected consistency check to succeed");
        assertThat(readReport(result))
                .contains(
                        "WARN  Index was dirty on startup which means it was not shutdown correctly and need to be cleaned up with a successful recovery.");
    }

    private static String readReport(ConsistencyCheckService.Result result) throws IOException {
        return Files.readString(result.reportFile());
    }

    void someData(EntityType entityType) {
        someData(50, entityType);
    }

    void someData(int numberOfModifications, EntityType entityType) {
        try (Transaction tx = db.beginTx()) {
            randomModifications(tx, numberOfModifications, entityType);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            createIndex(tx, entityType);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
    }

    private void createIndex(Transaction tx, EntityType entityType) {
        if (entityType.equals(EntityType.NODE)) {
            tx.schema().indexFor(LABEL_ONE).on(PROPERTY_KEY).create();
        } else {
            tx.schema().indexFor(TYPE_ONE).on(PROPERTY_KEY).create();
        }
    }

    private void randomModifications(Transaction tx, int numberOfModifications, EntityType entityType) {
        if (entityType.equals(EntityType.NODE)) {
            randomNodeModifications(tx, numberOfModifications);
        } else {
            randomRelationshipModifications(tx, numberOfModifications);
        }
    }

    private void randomNodeModifications(Transaction tx, int numberOfModifications) {
        List<Pair<Long, Label[]>> existingNodes = new ArrayList<>();
        for (int i = 0; i < numberOfModifications; i++) {
            double selectModification = random.nextDouble();
            if (existingNodes.size() < ENTITY_COUNT_BASELINE || selectModification >= DELETE_RATIO + UPDATE_RATIO) {
                createNewNode(tx, existingNodes);
            } else if (selectModification < DELETE_RATIO) {
                deleteExistingNode(tx, existingNodes);
            } else {
                modifyLabelsOnExistingNode(tx, existingNodes);
            }
        }
    }

    private void randomRelationshipModifications(Transaction tx, int numberOfModifications) {
        List<Long> existingRelationships = new ArrayList<>();
        for (int i = 0; i < numberOfModifications; i++) {
            double selectModification = random.nextDouble();
            if (existingRelationships.size() < ENTITY_COUNT_BASELINE
                    || selectModification >= DELETE_RATIO + UPDATE_RATIO) {
                createNewRelationship(tx, existingRelationships);
            } else if (selectModification < DELETE_RATIO) {
                deleteExistingRelationship(tx, existingRelationships);
            } else {
                modifyPropertiesOnExistingRelationship(tx, existingRelationships);
            }
        }
    }

    private void createNewNode(Transaction transaction, List<Pair<Long, Label[]>> existingNodes) {
        Label[] labels = randomLabels();
        Node node = createNewNode(transaction, labels);
        existingNodes.add(Pair.of(node.getId(), labels));
    }

    private Node createNewNode(Transaction tx, Label[] labels) {
        Node node = tx.createNode(labels);
        node.setProperty(PROPERTY_KEY, random.nextInt());
        return node;
    }

    private void createNewEntity(Transaction tx, EntityType entityType) {
        if (entityType.equals(EntityType.NODE)) {
            createNewNode(tx, new Label[] {LABEL_ONE});
        } else {
            Node node = tx.createNode();
            set(node.createRelationshipTo(node, TYPE_ONE), property(PROPERTY_KEY, random.nextInt()));
        }
    }

    private void createNewRelationship(Transaction transaction, List<Long> existingRelationships) {
        Node node = transaction.createNode();
        existingRelationships.add(set(
                        node.createRelationshipTo(node, random.among(TYPES)),
                        property(random.among(PROPERTY_KEYS), random.nextInt()))
                .getId());
    }

    private void modifyLabelsOnExistingNode(Transaction transaction, List<Pair<Long, Label[]>> existingNodes) {
        int targetIndex = random.nextInt(existingNodes.size());
        Pair<Long, Label[]> existingPair = existingNodes.get(targetIndex);
        long nodeId = existingPair.first();
        Node node = transaction.getNodeById(nodeId);
        node.getLabels().forEach(node::removeLabel);
        Label[] newLabels = randomLabels();
        for (Label label : newLabels) {
            node.addLabel(label);
        }
        existingNodes.remove(targetIndex);
        existingNodes.add(Pair.of(nodeId, newLabels));
    }

    private void modifyPropertiesOnExistingRelationship(Transaction transaction, List<Long> existingRelationships) {
        int targetIndex = random.nextInt(existingRelationships.size());
        long relId = existingRelationships.get(targetIndex);
        Relationship relationship = transaction.getRelationshipById(relId);
        relationship.getPropertyKeys().forEach(relationship::removeProperty);
        String[] properties = random.selection(PROPERTY_KEYS, 0, PROPERTY_KEYS.length, false);
        for (String property : properties) {
            relationship.setProperty(property, random.nextInt());
        }
    }

    private void deleteExistingNode(Transaction transaction, List<Pair<Long, Label[]>> existingNodes) {
        int targetIndex = random.nextInt(existingNodes.size());
        Pair<Long, Label[]> existingPair = existingNodes.get(targetIndex);
        Node node = transaction.getNodeById(existingPair.first());
        node.delete();
        existingNodes.remove(targetIndex);
    }

    private void deleteExistingRelationship(Transaction transaction, List<Long> existingRelationship) {
        int targetIndex = random.nextInt(existingRelationship.size());
        Relationship relationship = transaction.getRelationshipById(existingRelationship.get(targetIndex));
        relationship.delete();
        existingRelationship.remove(targetIndex);
    }

    private Label[] randomLabels() {
        List<Label> labels = new ArrayList<>(LABELS.length);
        for (Label label : LABELS) {
            if (random.nextBoolean()) {
                labels.add(label);
            }
        }
        return labels.toArray(new Label[0]);
    }

    private ConsistencyCheckService.Result fullConsistencyCheck()
            throws ConsistencyCheckIncompleteException, IOException {
        try (FileSystemAbstraction fsa = new DefaultFileSystemAbstraction()) {
            DatabaseLayout databaseLayout = db.databaseLayout();
            Config config = Config.defaults(logs_directory, databaseLayout.databaseDirectory());
            return new ConsistencyCheckService(databaseLayout)
                    .with(config)
                    .with(log)
                    .runFullConsistencyCheck();
        }
    }
}
