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
package org.neo4j.index.backup;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.index.IndexFileNames;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@DbmsExtension
@ExtendWith(RandomExtension.class)
public class IndexBackupIT {
    private static final String PROPERTY_PREFIX = "property";
    private static final int NUMBER_OF_INDEXES = 10;

    @Inject
    private RandomSupport random;

    @Inject
    private GraphDatabaseAPI database;

    private CheckPointer checkPointer;
    private IndexingService indexingService;
    private FileSystemAbstraction fileSystem;

    @Nested
    @DbmsExtension
    class LuceneIndexSnapshots {
        @Test
        void concurrentLuceneIndexSnapshotUseDifferentSnapshots() throws Exception {
            Label label = Label.label("testLabel");
            prepareDatabase(label);

            forceCheckpoint(checkPointer);
            ResourceIterator<Path> firstCheckpointSnapshot = indexingService.snapshotIndexFiles();
            generateData(label);
            removeOldNodes(LongStream.range(1, 20));
            updateOldNodes(LongStream.range(30, 40));

            forceCheckpoint(checkPointer);
            ResourceIterator<Path> secondCheckpointSnapshot = indexingService.snapshotIndexFiles();

            generateData(label);
            removeOldNodes(LongStream.range(50, 60));
            updateOldNodes(LongStream.range(70, 80));

            forceCheckpoint(checkPointer);
            ResourceIterator<Path> thirdCheckpointSnapshot = indexingService.snapshotIndexFiles();

            Set<String> firstSnapshotFileNames = getFileNames(firstCheckpointSnapshot);
            Set<String> secondSnapshotFileNames = getFileNames(secondCheckpointSnapshot);
            Set<String> thirdSnapshotFileNames = getFileNames(thirdCheckpointSnapshot);

            compareSnapshotFiles(firstSnapshotFileNames, secondSnapshotFileNames, fileSystem);
            compareSnapshotFiles(secondSnapshotFileNames, thirdSnapshotFileNames, fileSystem);
            compareSnapshotFiles(thirdSnapshotFileNames, firstSnapshotFileNames, fileSystem);

            firstCheckpointSnapshot.close();
            secondCheckpointSnapshot.close();
            thirdCheckpointSnapshot.close();
        }
    }

    @Test
    void snapshotFilesDeletedWhenSnapshotReleased() throws IOException {
        Label label = Label.label("testLabel");
        prepareDatabase(label);

        ResourceIterator<Path> firstCheckpointSnapshot = indexingService.snapshotIndexFiles();
        generateData(label);
        ResourceIterator<Path> secondCheckpointSnapshot = indexingService.snapshotIndexFiles();
        generateData(label);
        ResourceIterator<Path> thirdCheckpointSnapshot = indexingService.snapshotIndexFiles();

        Set<String> firstSnapshotFileNames = getFileNames(firstCheckpointSnapshot);
        Set<String> secondSnapshotFileNames = getFileNames(secondCheckpointSnapshot);
        Set<String> thirdSnapshotFileNames = getFileNames(thirdCheckpointSnapshot);

        generateData(label);
        forceCheckpoint(checkPointer);

        assertTrue(firstSnapshotFileNames.stream().map(Path::of).allMatch(file5 -> fileSystem.fileExists(file5)));
        assertTrue(secondSnapshotFileNames.stream().map(Path::of).allMatch(file4 -> fileSystem.fileExists(file4)));
        assertTrue(thirdSnapshotFileNames.stream().map(Path::of).allMatch(file3 -> fileSystem.fileExists(file3)));

        firstCheckpointSnapshot.close();
        secondCheckpointSnapshot.close();
        thirdCheckpointSnapshot.close();

        generateData(label);
        forceCheckpoint(checkPointer);

        assertFalse(firstSnapshotFileNames.stream().map(Path::of).anyMatch(file2 -> fileSystem.fileExists(file2)));
        assertFalse(secondSnapshotFileNames.stream().map(Path::of).anyMatch(file1 -> fileSystem.fileExists(file1)));
        assertFalse(thirdSnapshotFileNames.stream().map(Path::of).anyMatch(file -> fileSystem.fileExists(file)));
    }

    private static void compareSnapshotFiles(
            Set<String> firstSnapshotFileNames, Set<String> secondSnapshotFileNames, FileSystemAbstraction fileSystem) {
        assertThat(firstSnapshotFileNames)
                .as(format(
                        "Should have %d modified index segment files. Snapshot segment files are: %s",
                        NUMBER_OF_INDEXES, firstSnapshotFileNames))
                .hasSize(NUMBER_OF_INDEXES);
        for (String fileName : firstSnapshotFileNames) {
            assertFalse(
                    secondSnapshotFileNames.contains(fileName),
                    "Snapshot segments fileset should not have files from another snapshot set."
                            + describeFileSets(firstSnapshotFileNames, secondSnapshotFileNames));
            String path = FilenameUtils.getFullPath(fileName);
            assertTrue(
                    secondSnapshotFileNames.stream().anyMatch(name -> name.startsWith(path)),
                    "Snapshot should contain files for index in path: " + path + "."
                            + describeFileSets(firstSnapshotFileNames, secondSnapshotFileNames));
            assertTrue(
                    fileSystem.fileExists(Path.of(fileName)),
                    format("Snapshot segment file '%s' should exist.", fileName));
        }
    }

    private void removeOldNodes(LongStream idRange) {
        try (Transaction transaction = database.beginTx()) {
            idRange.mapToObj(transaction::getNodeById).forEach(Node::delete);
            transaction.commit();
        }
    }

    private void updateOldNodes(LongStream idRange) {
        try (Transaction transaction = database.beginTx()) {
            List<Node> nodes = idRange.mapToObj(transaction::getNodeById).collect(Collectors.toList());
            for (int i = 0; i < NUMBER_OF_INDEXES; i++) {
                String propertyName = PROPERTY_PREFIX + i;
                nodes.forEach(node -> node.setProperty(propertyName, random.nextString()));
            }
            transaction.commit();
        }
    }

    private static String describeFileSets(Set<String> firstFileSet, Set<String> secondFileSet) {
        return "First snapshot files are: " + firstFileSet + System.lineSeparator() + "second snapshot files are: "
                + secondFileSet;
    }

    private static Set<String> getFileNames(ResourceIterator<Path> files) {
        return files.stream()
                .map(Path::toAbsolutePath)
                .map(Path::toString)
                .filter(IndexBackupIT::segmentsFilePredicate)
                .collect(Collectors.toSet());
    }

    private static void forceCheckpoint(CheckPointer checkPointer) throws IOException {
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("testForcedCheckpoint"));
    }

    private void prepareDatabase(Label label) {
        generateData(label);

        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < 10; i++) {
                transaction
                        .schema()
                        .indexFor(label)
                        .on(PROPERTY_PREFIX + i)
                        .withIndexType(IndexType.TEXT)
                        .create();
            }
            transaction.commit();
        }

        try (Transaction tx = database.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
        }

        checkPointer = resolveDependency(CheckPointer.class);
        indexingService = resolveDependency(IndexingService.class);
        fileSystem = resolveDependency(FileSystemAbstraction.class);
    }

    private void generateData(Label label) {
        for (int i = 0; i < 100; i++) {
            testNodeCreationTransaction(label, i);
        }
    }

    private void testNodeCreationTransaction(Label label, int i) {
        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode(label);
            node.setProperty("property" + i, "" + i);
            transaction.commit();
        }
    }

    private <T> T resolveDependency(Class<T> clazz) {
        return getDatabaseResolver().resolveDependency(clazz);
    }

    private DependencyResolver getDatabaseResolver() {
        return database.getDependencyResolver();
    }

    private static boolean segmentsFilePredicate(String fileName) {
        return FilenameUtils.getName(fileName).startsWith(IndexFileNames.SEGMENTS);
    }
}
