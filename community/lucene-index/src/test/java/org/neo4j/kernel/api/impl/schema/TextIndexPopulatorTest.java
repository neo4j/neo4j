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

import static java.lang.Long.parseLong;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.LongStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexQueryHelper;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class TextIndexPopulatorTest {
    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDir;

    private TextIndexProvider provider;
    private Directory directory;
    private IndexPopulator indexPopulator;
    private IndexReader reader;
    private IndexSearcher searcher;
    private static final int propertyKeyId = 666;
    private IndexDescriptor index;

    @BeforeEach
    void before() throws IOException {
        directory = new ByteBuffersDirectory();
        DirectoryFactory directoryFactory =
                new DirectoryFactory.Single(new DirectoryFactory.UncloseableDirectory(directory));
        provider = new TextIndexProvider(
                fs,
                directoryFactory,
                directoriesByProvider(testDir.directory("folder")),
                new Monitors(),
                Config.defaults(),
                writable());
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig(Config.defaults());
        index = IndexPrototype.forSchema(forLabel(42, propertyKeyId), provider.getProviderDescriptor())
                .withName("index")
                .materialise(0)
                .withIndexCapability(TextIndexProvider.CAPABILITY);
        indexPopulator = provider.getPopulator(
                index,
                samplingConfig,
                heapBufferFactory(1024),
                INSTANCE,
                SIMPLE_TOKEN_LOOKUP,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        indexPopulator.create();
    }

    @AfterEach
    void after() throws Exception {
        if (reader != null) {
            reader.close();
        }
        directory.close();
    }

    @Test
    void addingValuesShouldPersistThem() throws Exception {
        // WHEN
        addUpdate(indexPopulator, 1, "First");
        addUpdate(indexPopulator, 2, "Second");
        addUpdate(indexPopulator, 3, "(byte) 1");
        addUpdate(indexPopulator, 4, "(short) 2");
        addUpdate(indexPopulator, 5, "3");
        addUpdate(indexPopulator, 6, "4L");
        addUpdate(indexPopulator, 7, "5F");
        addUpdate(indexPopulator, 8, "6D");

        // THEN
        assertIndexedValues(
                hit("First", 1),
                hit("Second", 2),
                hit("(byte) 1", 3),
                hit("(short) 2", 4),
                hit("3", 5),
                hit("4L", 6),
                hit("5F", 7),
                hit("6D", 8));
    }

    @Test
    void shouldIgnoreAddingUnsupportedValueTypes() throws Exception {
        // given  populating an empty index
        final var ids = LongStream.range(0L, 10L).toArray();

        // when   updates of unsupported value types (longs in this case) are processed
        final var updates = Arrays.stream(ids).mapToObj(id -> add(id, id)).toList();
        indexPopulator.add(updates, NULL_CONTEXT);

        // then   should not be indexed
        final var hits = Arrays.stream(ids).mapToObj(Hit::new).toArray(Hit[]::new);
        assertIndexedValues(hits);
    }

    @Test
    void multipleEqualValues() throws Exception {
        // WHEN
        addUpdate(indexPopulator, 1, "value");
        addUpdate(indexPopulator, 2, "value");
        addUpdate(indexPopulator, 3, "value");

        // THEN
        assertIndexedValues(hit("value", 1L, 2L, 3L));
    }

    @Test
    void multipleEqualValuesWithUpdateThatRemovesOne() throws Exception {
        // WHEN
        addUpdate(indexPopulator, 1, "value");
        addUpdate(indexPopulator, 2, "value");
        addUpdate(indexPopulator, 3, "value");
        updatePopulator(indexPopulator, singletonList(remove(2, "value")));

        // THEN
        assertIndexedValues(hit("value", 1L, 3L));
    }

    @Test
    void changeUpdatesInterleavedWithAdds() throws Exception {
        // WHEN
        addUpdate(indexPopulator, 1, "1");
        addUpdate(indexPopulator, 2, "2");
        updatePopulator(indexPopulator, singletonList(change(1, "1", "1a")));
        addUpdate(indexPopulator, 3, "3");

        // THEN
        assertIndexedValues(no("1"), hit("1a", 1), hit("2", 2), hit("3", 3));
    }

    @Test
    void addUpdatesInterleavedWithAdds() throws Exception {
        // WHEN
        addUpdate(indexPopulator, 1, "1");
        addUpdate(indexPopulator, 2, "2");
        updatePopulator(indexPopulator, asList(remove(1, "1"), add(1, "1a")));
        addUpdate(indexPopulator, 3, "3");

        // THEN
        assertIndexedValues(hit("1a", 1), hit("2", 2), hit("3", 3), no("1"));
    }

    @Test
    void removeUpdatesInterleavedWithAdds() throws Exception {
        // WHEN
        addUpdate(indexPopulator, 1, "1");
        addUpdate(indexPopulator, 2, "2");
        updatePopulator(indexPopulator, singletonList(remove(2, "2")));
        addUpdate(indexPopulator, 3, "3");

        // THEN
        assertIndexedValues(hit("1", 1), no("2"), hit("3", 3));
    }

    @Test
    void multipleInterleaves() throws Exception {
        // WHEN
        addUpdate(indexPopulator, 1, "1");
        addUpdate(indexPopulator, 2, "2");
        updatePopulator(indexPopulator, asList(change(1, "1", "1a"), change(2, "2", "2a")));
        addUpdate(indexPopulator, 3, "3");
        addUpdate(indexPopulator, 4, "4");
        updatePopulator(indexPopulator, asList(change(1, "1a", "1b"), change(4, "4", "4a")));

        // THEN
        assertIndexedValues(no("1"), no("1a"), hit("1b", 1), no("2"), hit("2a", 2), hit("3", 3), no("4"), hit("4a", 4));
    }

    private static Hit hit(Object value, Long... nodeIds) {
        return new Hit(value, nodeIds);
    }

    private static Hit hit(Object value, long nodeId) {
        return new Hit(value, nodeId);
    }

    private static Hit no(Object value) {
        return new Hit(value);
    }

    private static class Hit {
        private final Value value;
        private final Long[] nodeIds;

        Hit(Object value, Long... nodeIds) {
            this.value = Values.of(value);
            this.nodeIds = nodeIds;
        }
    }

    private IndexEntryUpdate<?> add(long nodeId, Object value) {
        return IndexQueryHelper.add(nodeId, index, value);
    }

    private IndexEntryUpdate<?> change(long nodeId, Object valueBefore, Object valueAfter) {
        return IndexQueryHelper.change(nodeId, index, valueBefore, valueAfter);
    }

    private IndexEntryUpdate<?> remove(long nodeId, Object removedValue) {
        return IndexQueryHelper.remove(nodeId, index, removedValue);
    }

    private void assertIndexedValues(Hit... expectedHits) throws IOException {
        switchToVerification();

        for (Hit hit : expectedHits) {
            TopDocs hits = searcher.search(TextDocumentStructure.newSeekQuery(hit.value), 10);
            assertEquals(TotalHits.Relation.EQUAL_TO, hits.totalHits.relation);
            assertEquals(
                    hit.nodeIds.length, hits.totalHits.value, "Unexpected number of index results from " + hit.value);
            Set<Long> foundNodeIds = new HashSet<>();
            for (int i = 0; i < hits.totalHits.value; i++) {
                Document document = searcher.doc(hits.scoreDocs[i].doc);
                foundNodeIds.add(parseLong(document.get("id")));
            }
            assertEquals(asSet(hit.nodeIds), foundNodeIds);
        }
    }

    private void switchToVerification() throws IOException {
        indexPopulator.close(true, NULL_CONTEXT);
        assertEquals(InternalIndexState.ONLINE, provider.getInitialState(index, NULL_CONTEXT, Sets.immutable.empty()));
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    private void addUpdate(IndexPopulator populator, long nodeId, Object value) throws IndexEntryConflictException {
        populator.add(singletonList(IndexQueryHelper.add(nodeId, index, value)), NULL_CONTEXT);
    }

    private static void updatePopulator(IndexPopulator populator, Iterable<IndexEntryUpdate<?>> updates)
            throws IndexEntryConflictException {
        try (IndexUpdater updater = populator.newPopulatingUpdater(NULL_CONTEXT)) {
            for (IndexEntryUpdate<?> update : updates) {
                updater.process(update);
            }
        }
    }
}
