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
package org.neo4j.kernel.api.impl.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.async.AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
import static org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider.UPDATE_IGNORE_STRATEGY;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSettings;
import org.neo4j.kernel.api.impl.schema.text.TextIndexAccessor;
import org.neo4j.kernel.api.impl.schema.text.TextIndexBuilder;
import org.neo4j.kernel.api.impl.schema.text.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class TextIndexPopulationIT {
    private static final IndexDescriptor DESCRIPTOR = IndexPrototype.uniqueForSchema(SchemaDescriptors.forLabel(0, 0))
            .withName("a")
            .withIndexType(IndexType.TEXT)
            .withIndexProvider(AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR)
            .materialise(1)
            .withIndexCapability(TextIndexProvider.CAPABILITY);
    private static final Config CONFIG = Config.newBuilder()
            .set(LuceneSettings.lucene_max_partition_size, 10)
            .build();

    @Inject
    private TestDirectory testDir;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private static Stream<Arguments> provideParameters() {
        return Stream.of(
                Arguments.of(LuceneContext.LUCENE_9, 7),
                Arguments.of(LuceneContext.LUCENE_9, 11),
                Arguments.of(LuceneContext.LUCENE_9, 14),
                Arguments.of(LuceneContext.LUCENE_9, 20),
                Arguments.of(LuceneContext.LUCENE_9, 35),
                Arguments.of(LuceneContext.LUCENE_9, 58),
                Arguments.of(LuceneContext.LUCENE_10, 7),
                Arguments.of(LuceneContext.LUCENE_10, 11),
                Arguments.of(LuceneContext.LUCENE_10, 14),
                Arguments.of(LuceneContext.LUCENE_10, 20),
                Arguments.of(LuceneContext.LUCENE_10, 35),
                Arguments.of(LuceneContext.LUCENE_10, 58));
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    void partitionedIndexPopulation(LuceneContext luceneContext, int affectedNodes) throws Exception {
        Path rootFolder = testDir.directory("partitionIndex" + affectedNodes).resolve("uniqueIndex" + affectedNodes);
        try (DatabaseIndex<ValueIndexReader> index = TextIndexBuilder.create(
                        DESCRIPTOR, writable(), CONFIG, NullLogProvider.getInstance())
                .withFileSystem(fileSystem)
                .withLuceneContext(luceneContext)
                .withIndexRootFolder(rootFolder)
                .build()) {
            index.open();

            // index is empty and not yet exist
            assertEquals(0, index.allDocumentsReader().maxCount());
            assertFalse(index.exists());

            try (TextIndexAccessor indexAccessor =
                    new TextIndexAccessor(index, DESCRIPTOR, SIMPLE_TOKEN_LOOKUP, null, UPDATE_IGNORE_STRATEGY)) {
                generateUpdates(indexAccessor, affectedNodes);
                indexAccessor.force(FileFlushEvent.NULL, EMPTY_ASYNC_BLOCK_ACCESSOR, CursorContext.NULL_CONTEXT);

                // now index is online and should contain updates data
                assertTrue(index.isOnline());

                try (ValueIndexReader indexReader = indexAccessor.newValueReader(NO_USAGE_TRACKING);
                        NodeValueIterator results = new NodeValueIterator();
                        IndexSampler indexSampler = indexReader.createSampler()) {
                    indexReader.query(
                            results,
                            QueryContext.NULL_CONTEXT,
                            CursorContext.NULL_CONTEXT,
                            unconstrained(),
                            PropertyIndexQuery.allEntries());
                    long[] nodes = PrimitiveLongCollections.asArray(results);
                    assertEquals(affectedNodes, nodes.length);

                    IndexSample sample = indexSampler.sampleIndex(CursorContext.NULL_CONTEXT, new AtomicBoolean());
                    assertEquals(affectedNodes, sample.indexSize());
                    assertEquals(affectedNodes, sample.uniqueValues());
                    assertEquals(affectedNodes, sample.sampleSize());
                }
            }
        }
    }

    private void generateUpdates(TextIndexAccessor indexAccessor, int nodesToUpdate)
            throws IndexEntryConflictException {
        try (IndexUpdater updater =
                indexAccessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
            for (int nodeId = 0; nodeId < nodesToUpdate; nodeId++) {
                updater.process(add(nodeId, "node " + nodeId));
            }
        }
    }

    private IndexEntryUpdate add(long nodeId, Object value) {
        return EagerValueIndexEntryUpdate.add(nodeId, DESCRIPTOR, Values.of(value));
    }
}
