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
package org.neo4j.kernel.api.impl.schema;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider.UPDATE_IGNORE_STRATEGY;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.LuceneAllDocumentsReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class TextIndexIT {

    @Inject
    private TestDirectory testDir;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private final IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(0, 0))
            .withName("a")
            .withIndexType(IndexType.TEXT)
            .withIndexProvider(AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR)
            .materialise(1);
    private final Config config = Config.newBuilder()
            .set(GraphDatabaseInternalSettings.lucene_max_partition_size, 10)
            .build();

    @Test
    void snapshotForPartitionedIndex() throws Exception {
        // Given
        try (TextIndexAccessor indexAccessor = createDefaultIndexAccessor()) {
            generateUpdates(indexAccessor, 32);
            indexAccessor.force(FileFlushEvent.NULL, NULL_CONTEXT);

            // When & Then
            List<String> indexFileNames = asFileInsidePartitionNames(indexAccessor.snapshotFiles());

            List<String> singlePartitionFileTemplates = Arrays.asList(".cfe", ".cfs", ".si", "segments_1");
            assertTrue(
                    indexFileNames.size() >= (singlePartitionFileTemplates.size() * 4),
                    "Expect files from 4 partitions");
            Map<String, Integer> templateMatches = countTemplateMatches(singlePartitionFileTemplates, indexFileNames);

            for (String fileTemplate : singlePartitionFileTemplates) {
                Integer matches = templateMatches.get(fileTemplate);
                assertTrue(matches >= 4, "Expect to see at least 4 matches for template: " + fileTemplate);
            }
        }
    }

    @Test
    void snapshotForIndexWithNoCommits() throws Exception {
        // Given
        // A completely un-used index
        try (TextIndexAccessor indexAccessor = createDefaultIndexAccessor();
                ResourceIterator<Path> snapshotIterator = indexAccessor.snapshotFiles()) {
            assertThat(asUniqueSetOfNames(snapshotIterator)).isEqualTo(emptySet());
        }
    }

    @Test
    void updateMultiplePartitionedIndex() throws IOException {
        try (var index = TextIndexBuilder.create(descriptor, writable(), config)
                .withFileSystem(fileSystem)
                .withIndexRootFolder(testDir.directory("partitionedIndexForUpdates"))
                .build()) {
            index.create();
            index.open();
            addDocumentToIndex(index, 45);

            index.getIndexWriter()
                    .updateDocument(
                            TextDocumentStructure.newTermForChangeOrRemove(100),
                            TextDocumentStructure.documentRepresentingProperties(100, Values.stringValue("100")));
            index.maybeRefreshBlocking();

            long documentsInIndex = Iterators.count(index.allDocumentsReader().iterator());
            assertEquals(46, documentsInIndex, "Index should contain 45 added and 1 updated document.");
        }
    }

    @Test
    void createPopulateDropIndex() throws Exception {
        Path crudOperation = testDir.directory("indexCRUDOperation");
        try (var crudIndex = TextIndexBuilder.create(descriptor, writable(), config)
                .withFileSystem(fileSystem)
                .withIndexRootFolder(crudOperation.resolve("crudIndex"))
                .build()) {
            crudIndex.open();

            addDocumentToIndex(crudIndex, 1);
            assertEquals(1, crudIndex.getPartitions().size());

            addDocumentToIndex(crudIndex, 21);
            assertEquals(3, crudIndex.getPartitions().size());

            crudIndex.drop();

            assertFalse(crudIndex.isOpen());
            assertEquals(0, crudOperation.toFile().list().length);
        }
    }

    @Test
    void createFailPartitionedIndex() throws Exception {
        try (var failedIndex = TextIndexBuilder.create(descriptor, writable(), config)
                .withFileSystem(fileSystem)
                .withIndexRootFolder(testDir.directory("failedIndexFolder").resolve("failedIndex"))
                .build()) {
            failedIndex.open();

            addDocumentToIndex(failedIndex, 35);
            assertEquals(4, failedIndex.getPartitions().size());

            failedIndex.markAsFailed("Some failure");
            failedIndex.flush();

            assertTrue(failedIndex.isOpen());
            assertFalse(failedIndex.isOnline());
        }
    }

    @Test
    void openClosePartitionedIndex() throws IOException {
        Path indexRootFolder = testDir.directory("reopenIndexFolder").resolve("reopenIndex");
        TextIndexBuilder textIndexBuilder = TextIndexBuilder.create(descriptor, writable(), config)
                .withFileSystem(fileSystem)
                .withIndexRootFolder(indexRootFolder);
        try (var reopenIndex = textIndexBuilder.build()) {
            reopenIndex.open();

            addDocumentToIndex(reopenIndex, 1);

            reopenIndex.close();
            assertFalse(reopenIndex.isOpen());

            reopenIndex.open();
            assertTrue(reopenIndex.isOpen());

            addDocumentToIndex(reopenIndex, 10);

            reopenIndex.close();
            assertFalse(reopenIndex.isOpen());

            reopenIndex.open();
            assertTrue(reopenIndex.isOpen());

            reopenIndex.close();
            reopenIndex.open();
            addDocumentToIndex(reopenIndex, 100);

            reopenIndex.maybeRefreshBlocking();

            try (LuceneAllDocumentsReader allDocumentsReader = reopenIndex.allDocumentsReader()) {
                assertEquals(111, allDocumentsReader.maxCount(), "All documents should be visible");
            }
        }
    }

    private static void addDocumentToIndex(DatabaseIndex<ValueIndexReader> index, int documents) throws IOException {
        for (int i = 0; i < documents; i++) {
            index.getIndexWriter()
                    .addDocument(TextDocumentStructure.documentRepresentingProperties(i, Values.stringValue("" + i)));
        }
    }

    private TextIndexAccessor createDefaultIndexAccessor() throws IOException {
        var index = TextIndexBuilder.create(descriptor, writable(), config)
                .withFileSystem(fileSystem)
                .withIndexRootFolder(testDir.directory("testIndex"))
                .build();
        index.create();
        index.open();
        return new TextIndexAccessor(index, descriptor, SIMPLE_TOKEN_LOOKUP, UPDATE_IGNORE_STRATEGY);
    }

    private List<String> asFileInsidePartitionNames(ResourceIterator<Path> resources) {
        int testDirectoryPathLength = testDir.absolutePath().toString().length();
        return resources.stream()
                .map(file -> file.toAbsolutePath().toString().substring(testDirectoryPathLength))
                .toList();
    }

    private void generateUpdates(TextIndexAccessor indexAccessor, int nodesToUpdate)
            throws IndexEntryConflictException {
        try (IndexUpdater updater = indexAccessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
            for (int nodeId = 0; nodeId < nodesToUpdate; nodeId++) {
                updater.process(add(nodeId, "node " + nodeId));
            }
        }
    }

    private IndexEntryUpdate<?> add(long nodeId, Object value) {
        return IndexEntryUpdate.add(nodeId, descriptor, Values.of(value));
    }

    private static Map<String, Integer> countTemplateMatches(List<String> nameTemplates, List<String> fileNames) {
        Map<String, Integer> templateMatches = new HashMap<>();
        for (String indexFileName : fileNames) {
            for (String template : nameTemplates) {
                if (indexFileName.endsWith(template)) {
                    templateMatches.put(template, templateMatches.getOrDefault(template, 0) + 1);
                }
            }
        }
        return templateMatches;
    }

    private static Set<String> asUniqueSetOfNames(ResourceIterator<Path> files) {
        List<String> out = new ArrayList<>();
        while (files.hasNext()) {
            String name = files.next().getFileName().toString();
            out.add(name);
        }
        return Iterables.asUniqueSet(out);
    }
}
