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
import static org.neo4j.io.fs.FileUtils.copyFile;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.test.TestLabels.LABEL_ONE;
import static org.neo4j.test.TestLabels.LABEL_THREE;
import static org.neo4j.test.TestLabels.LABEL_TWO;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.IndexFiles;
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
class AllNodesInStoreExistInLabelIndexTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    DatabaseManagementService managementService;

    @Inject
    GraphDatabaseAPI db;

    @Inject
    private Database database;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private RandomSupport random;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, PageAligned.LATEST_NAME);
    }

    private final AssertableLogProvider log = new AssertableLogProvider();
    private static final Label[] LABEL_ALPHABET = new Label[] {LABEL_ONE, LABEL_TWO, LABEL_THREE};
    private static final Label EXTRA_LABEL = Label.label("extra");
    private static final double DELETE_RATIO = 0.2;
    private static final double UPDATE_RATIO = 0.2;
    private static final int NODE_COUNT_BASELINE = 10;

    @Test
    void mustReportSuccessfulForConsistentLabelTokenIndex() throws Exception {
        // given
        someData();
        managementService.shutdown();

        // when
        ConsistencyCheckService.Result result = fullConsistencyCheck();

        // then
        assertTrue(result.isSuccessful(), "Expected consistency check to succeed");
    }

    @Test
    void reportNotCleanLabelIndex() throws IOException, ConsistencyCheckIncompleteException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        someData();
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("forcedCheckpoint"));
        Path labelIndexFileCopy = databaseLayout.file("label_index_copy");
        Path labelTokenIndexFile = labelTokenIndexFile();
        copyFile(labelTokenIndexFile, labelIndexFileCopy);

        try (Transaction tx = db.beginTx()) {
            tx.createNode(LABEL_ONE);
            tx.commit();
        }

        managementService.shutdown();

        copyFile(labelIndexFileCopy, labelTokenIndexFile);

        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse(result.isSuccessful(), "Expected consistency check to fail");
        assertThat(readReport(result))
                .contains(
                        "WARN  Index was dirty on startup which means it was not shutdown correctly and need to be cleaned up with a successful recovery.");
    }

    @Test
    void reportNotCleanLabelIndexWithCorrectData() throws IOException, ConsistencyCheckIncompleteException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        someData();
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("forcedCheckpoint"));
        Path labelIndexFileCopy = databaseLayout.file("label_index_copy");
        Path labelTokenIndexFile = labelTokenIndexFile();
        copyFile(labelTokenIndexFile, labelIndexFileCopy);

        managementService.shutdown();

        copyFile(labelIndexFileCopy, labelTokenIndexFile);

        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertTrue(result.isSuccessful(), "Expected consistency check to fail");
        assertThat(readReport(result))
                .contains(
                        "WARN  Index was dirty on startup which means it was not shutdown correctly and need to be cleaned up with a successful recovery.");
    }

    @Test
    void mustReportMissingNode() throws Exception {
        // given
        someData();
        Path labelIndexFileCopy = copyLabelIndexFile();

        // when
        try (Transaction tx = db.beginTx()) {
            tx.createNode(LABEL_ONE);
            tx.commit();
        }

        // and
        replaceLabelIndexWithCopy(labelIndexFileCopy);

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse(result.isSuccessful(), "Expected consistency check to fail");
    }

    @Test
    void mustReportMissingLabel() throws Exception {
        // given
        List<Pair<Long, Label[]>> nodesInStore = someData();
        Path labelIndexFileCopy = copyLabelIndexFile();

        // when
        try (Transaction tx = db.beginTx()) {
            addLabelToExistingNode(tx, nodesInStore);
            tx.commit();
        }

        // and
        replaceLabelIndexWithCopy(labelIndexFileCopy);

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse(result.isSuccessful(), "Expected consistency check to fail");
    }

    @Test
    void mustReportExtraLabelsOnExistingNode() throws Exception {
        // given
        List<Pair<Long, Label[]>> nodesInStore = someData();
        Path labelIndexFileCopy = copyLabelIndexFile();

        // when
        try (Transaction tx = db.beginTx()) {
            removeLabelFromExistingNode(tx, nodesInStore);
            tx.commit();
        }

        // and
        replaceLabelIndexWithCopy(labelIndexFileCopy);

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse(result.isSuccessful(), "Expected consistency check to fail");
    }

    @Test
    void mustReportExtraNode() throws Exception {
        // given
        List<Pair<Long, Label[]>> nodesInStore = someData();
        Path labelIndexFileCopy = copyLabelIndexFile();

        // when
        try (Transaction tx = db.beginTx()) {
            removeExistingNode(tx, nodesInStore);
            tx.commit();
        }

        // and
        replaceLabelIndexWithCopy(labelIndexFileCopy);

        // then
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertFalse(result.isSuccessful(), "Expected consistency check to fail");
    }

    @Test
    void checkShouldBeSuccessfulIfNoNodeLabelIndexExist() throws ConsistencyCheckIncompleteException {
        // Remove the Node Label Index
        try (Transaction tx = db.beginTx()) {
            final Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
            for (IndexDefinition index : indexes) {
                if (index.getIndexType() == IndexType.LOOKUP && index.isNodeIndex()) {
                    index.drop();
                }
            }
            tx.commit();
        }

        // Add some data to the node store
        someData();
        managementService.shutdown();

        // Then consistency check should still be successful without NLI
        ConsistencyCheckService.Result result = fullConsistencyCheck();
        assertTrue(result.isSuccessful(), "Expected consistency check to succeed");
    }

    private static String readReport(ConsistencyCheckService.Result result) throws IOException {
        return Files.readString(result.reportFile());
    }

    private void removeExistingNode(Transaction transaction, List<Pair<Long, Label[]>> nodesInStore) {
        Node node;
        Label[] labels;
        do {
            int targetIndex = random.nextInt(nodesInStore.size());
            Pair<Long, Label[]> existingNode = nodesInStore.get(targetIndex);
            node = transaction.getNodeById(existingNode.first());
            labels = existingNode.other();
        } while (labels.length == 0);
        node.delete();
    }

    private void addLabelToExistingNode(Transaction transaction, List<Pair<Long, Label[]>> nodesInStore) {
        int targetIndex = random.nextInt(nodesInStore.size());
        Pair<Long, Label[]> existingNode = nodesInStore.get(targetIndex);
        Node node = transaction.getNodeById(existingNode.first());
        node.addLabel(EXTRA_LABEL);
    }

    private void removeLabelFromExistingNode(Transaction transaction, List<Pair<Long, Label[]>> nodesInStore) {
        Pair<Long, Label[]> existingNode;
        Node node;
        do {
            int targetIndex = random.nextInt(nodesInStore.size());
            existingNode = nodesInStore.get(targetIndex);
            node = transaction.getNodeById(existingNode.first());
        } while (existingNode.other().length == 0);
        node.removeLabel(existingNode.other()[0]);
    }

    private void replaceLabelIndexWithCopy(Path labelIndexFileCopy) throws IOException {
        Path labelTokenIndexFile = labelTokenIndexFile();
        managementService.shutdown();

        fs.deleteFile(labelTokenIndexFile);
        fs.copyFile(labelIndexFileCopy, labelTokenIndexFile);
    }

    private Path copyLabelIndexFile() throws IOException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        Path labelIndexFileCopy = databaseLayout.file("label_index_copy");
        Path labelTokenIndexFile = labelTokenIndexFile();
        database.stop();
        fs.copyFile(labelTokenIndexFile, labelIndexFileCopy);
        database.start();
        return labelIndexFileCopy;
    }

    List<Pair<Long, Label[]>> someData() {
        return someData(50);
    }

    private List<Pair<Long, Label[]>> someData(int numberOfModifications) {
        List<Pair<Long, Label[]>> existingNodes;
        existingNodes = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            randomModifications(tx, existingNodes, numberOfModifications);
            tx.commit();
        }
        return existingNodes;
    }

    private void randomModifications(
            Transaction tx, List<Pair<Long, Label[]>> existingNodes, int numberOfModifications) {
        for (int i = 0; i < numberOfModifications; i++) {
            double selectModification = random.nextDouble();
            if (existingNodes.size() < NODE_COUNT_BASELINE || selectModification >= DELETE_RATIO + UPDATE_RATIO) {
                createNewNode(tx, existingNodes);
            } else if (selectModification < DELETE_RATIO) {
                deleteExistingNode(tx, existingNodes);
            } else {
                modifyLabelsOnExistingNode(tx, existingNodes);
            }
        }
    }

    private void createNewNode(Transaction tx, List<Pair<Long, Label[]>> existingNodes) {
        Label[] labels = randomLabels();
        Node node = tx.createNode(labels);
        existingNodes.add(Pair.of(node.getId(), labels));
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

    private void deleteExistingNode(Transaction transaction, List<Pair<Long, Label[]>> existingNodes) {
        int targetIndex = random.nextInt(existingNodes.size());
        Pair<Long, Label[]> existingPair = existingNodes.get(targetIndex);
        Node node = transaction.getNodeById(existingPair.first());
        node.delete();
        existingNodes.remove(targetIndex);
    }

    private Label[] randomLabels() {
        List<Label> labels = new ArrayList<>(3);
        for (Label label : LABEL_ALPHABET) {
            if (random.nextBoolean()) {
                labels.add(label);
            }
        }
        return labels.toArray(new Label[0]);
    }

    Config addAdditionalConfigToCC(Config config) {
        return config;
    }

    ConsistencyCheckService.Result fullConsistencyCheck() throws ConsistencyCheckIncompleteException {
        DatabaseLayout databaseLayout = db.databaseLayout();
        Config config = Config.newBuilder()
                .set(logs_directory, databaseLayout.databaseDirectory())
                .build();
        return new ConsistencyCheckService(databaseLayout)
                .with(addAdditionalConfigToCC(config))
                .with(log)
                .runFullConsistencyCheck();
    }

    private Path labelTokenIndexFile() {
        try (var tx = db.beginTx()) {
            return StreamSupport.stream(tx.schema().getIndexes().spliterator(), false)
                    .filter(idx -> idx.getIndexType() == IndexType.LOOKUP)
                    .filter(IndexDefinition::isNodeIndex)
                    .map(idx -> {
                        IndexDirectoryStructure indexDirectoryStructure = directoriesByProvider(
                                        db.databaseLayout().databaseDirectory())
                                .forProvider(AllIndexProviderDescriptors.TOKEN_DESCRIPTOR);
                        long idxId =
                                ((IndexDefinitionImpl) idx).getIndexReference().getId();
                        IndexFiles indexFiles = new IndexFiles(fs, indexDirectoryStructure, idxId);
                        return indexFiles.getStoreFile();
                    })
                    .findAny()
                    .get();
        }
    }
}
