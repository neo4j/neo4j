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
import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.PERSISTENT;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.LuceneMinimalIndexAccessor;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LuceneMinimalIndexAccessorTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void shouldSnapshotFailureFileOnFailedIndex() throws IOException {
        var indexDescriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2), TextIndexProvider.DESCRIPTOR)
                .withName("failure")
                .materialise(5);
        var storage = new PartitionedIndexStorage(PERSISTENT, fs, directory.directory("root"));

        var readOnlyChecker = DatabaseReadOnlyChecker.writable();
        try (var index = TextIndexBuilder.create(indexDescriptor, readOnlyChecker, Config.defaults())
                .withIndexStorage(storage)
                .withFileSystem(fs)
                .build()) {
            index.create();
            index.markAsFailed("This is a failure");
            var accessor = new LuceneMinimalIndexAccessor<>(indexDescriptor, index, false);
            try (var snapshot = accessor.snapshotFiles()) {
                var failureFile = single(snapshot);
                assertThat(failureFile.toString()).contains("failure");
            }
        }
    }

    @Test
    void shouldSnapshotIndexFileOnOnline() throws IOException {
        var indexDescriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2), TextIndexProvider.DESCRIPTOR)
                .withName("failure")
                .materialise(5);
        var storage = new PartitionedIndexStorage(PERSISTENT, fs, directory.directory("root"));

        var readOnlyChecker = DatabaseReadOnlyChecker.writable();
        try (var index = TextIndexBuilder.create(indexDescriptor, readOnlyChecker, Config.defaults())
                .withIndexStorage(storage)
                .withFileSystem(fs)
                .build()) {
            index.create();
            index.open();
            var accessor = new LuceneMinimalIndexAccessor<>(indexDescriptor, index, false);
            try (var snapshot = accessor.snapshotFiles()) {
                while (snapshot.hasNext()) {
                    var file = snapshot.next();
                    assertThat(file.toString()).doesNotContain("failure");
                }
            }
        }
    }
}
