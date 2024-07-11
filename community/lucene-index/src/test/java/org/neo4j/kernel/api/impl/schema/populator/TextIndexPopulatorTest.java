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
package org.neo4j.kernel.api.impl.schema.populator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider.UPDATE_IGNORE_STRATEGY;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.TextIndexBuilder;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class TextIndexPopulatorTest {
    private final DirectoryFactory dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();

    @Inject
    private TestDirectory testDir;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    private DatabaseIndex<ValueIndexReader> index;
    private TextIndexPopulator populator;
    private final SchemaDescriptorSupplier labelSchemaDescriptor = () -> SchemaDescriptors.forLabel(0, 0);

    @BeforeEach
    void setUp() {
        Path folder = testDir.directory("folder");
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage(dirFactory, fileSystem, folder);

        IndexDescriptor descriptor = IndexPrototype.forSchema(labelSchemaDescriptor.schema())
                .withName("index")
                .withIndexType(IndexType.TEXT)
                .withIndexProvider(TextIndexProvider.DESCRIPTOR)
                .materialise(13)
                .withIndexCapability(TextIndexProvider.CAPABILITY);
        index = TextIndexBuilder.create(descriptor, writable(), Config.defaults())
                .withIndexStorage(indexStorage)
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (populator != null) {
            populator.close(false, CursorContext.NULL_CONTEXT);
        }
        IOUtils.closeAll(index, dirFactory);
    }

    @Test
    void sampleEmptyIndex() {
        populator = newPopulator();

        IndexSample sample = populator.sample(CursorContext.NULL_CONTEXT);

        assertEquals(new IndexSample(), sample);
    }

    @Test
    void sampleIncludedUpdates() {
        populator = newPopulator();
        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add(1, labelSchemaDescriptor, "aaa"),
                add(2, labelSchemaDescriptor, "bbb"),
                add(3, labelSchemaDescriptor, "ccc"));
        populator.add(updates, CursorContext.NULL_CONTEXT);

        IndexSample sample = populator.sample(CursorContext.NULL_CONTEXT);

        assertEquals(new IndexSample(3, 3, 3), sample);
    }

    @Test
    void sampleIncludedUpdatesWithDuplicates() {
        populator = newPopulator();
        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add(1, labelSchemaDescriptor, "foo"),
                add(2, labelSchemaDescriptor, "bar"),
                add(3, labelSchemaDescriptor, "foo"));
        populator.add(updates, CursorContext.NULL_CONTEXT);

        IndexSample sample = populator.sample(CursorContext.NULL_CONTEXT);

        assertEquals(new IndexSample(3, 2, 3), sample);
    }

    @Test
    void addUpdates() throws Exception {
        populator = newPopulator();

        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add(1, labelSchemaDescriptor, "foo"),
                add(2, labelSchemaDescriptor, "bar"),
                add(42, labelSchemaDescriptor, "bar"));

        populator.add(updates, CursorContext.NULL_CONTEXT);

        index.maybeRefreshBlocking();
        try (ValueIndexReader reader = index.getIndexReader(NO_USAGE_TRACKING);
                NodeValueIterator allEntities = new NodeValueIterator()) {
            reader.query(allEntities, QueryContext.NULL_CONTEXT, unconstrained(), PropertyIndexQuery.allEntries());
            assertArrayEquals(new long[] {1, 2, 42}, PrimitiveLongCollections.asArray(allEntities));
        }
    }

    private TextIndexPopulator newPopulator() {
        TextIndexPopulator populator = new TextIndexPopulator(index, UPDATE_IGNORE_STRATEGY);
        populator.create();
        return populator;
    }
}
