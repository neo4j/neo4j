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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.single;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.IndexFileSnapshotter;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.LuceneMinimalIndexAccessor;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.text.TextIndexBuilder;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LuceneMinimalIndexAccessorTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @ParameterizedTest
    @EnumSource
    void shouldSnapshotFailureFileOnFailedIndex(LuceneContext luceneContext) throws IOException {
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                        SchemaDescriptors.forLabel(1, 2), AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR)
                .withName("failure")
                .materialise(5);
        PartitionedIndexStorage storage = new PartitionedIndexStorage(
                luceneContext, DirectoryFactory.persistent(luceneContext), fs, directory.directory("root"));

        DatabaseReadOnlyChecker readOnlyChecker = DatabaseReadOnlyChecker.writable();
        try (DatabaseIndex<ValueIndexReader> index = TextIndexBuilder.create(
                        indexDescriptor, readOnlyChecker, Config.defaults(), NullLogProvider.getInstance())
                .withIndexStorage(storage)
                .withFileSystem(fs)
                .build()) {
            index.create();
            index.markAsFailed("This is a failure");
            IndexFileSnapshotter accessor = new LuceneMinimalIndexAccessor<>(indexDescriptor, index, false);
            try (ResourceIterator<Path> snapshot = accessor.snapshotFiles()) {
                Path failureFile = single(snapshot);
                assertThat(failureFile.toString()).contains("failure");
            }
        }
    }

    @ParameterizedTest
    @EnumSource
    void shouldSnapshotIndexFileOnOnline(LuceneContext luceneContext) throws IOException {
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                        SchemaDescriptors.forLabel(1, 2), AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR)
                .withName("failure")
                .materialise(5);
        PartitionedIndexStorage storage = new PartitionedIndexStorage(
                luceneContext, DirectoryFactory.persistent(luceneContext), fs, directory.directory("root"));

        DatabaseReadOnlyChecker readOnlyChecker = DatabaseReadOnlyChecker.writable();
        try (DatabaseIndex<ValueIndexReader> index = TextIndexBuilder.create(
                        indexDescriptor, readOnlyChecker, Config.defaults(), NullLogProvider.getInstance())
                .withIndexStorage(storage)
                .withFileSystem(fs)
                .build()) {
            index.create();
            index.open();
            IndexFileSnapshotter accessor = new LuceneMinimalIndexAccessor<>(indexDescriptor, index, false);
            try (ResourceIterator<Path> snapshot = accessor.snapshotFiles()) {
                while (snapshot.hasNext()) {
                    Path file = snapshot.next();
                    assertThat(file.toString()).doesNotContain("failure");
                }
            }
        }
    }
}
