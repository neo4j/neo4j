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
package org.neo4j.kernel.impl.index.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class IndexUpdateStorageTest {
    private static final SchemaDescriptorSupplier descriptor = () -> SchemaDescriptors.forLabel(1, 1);

    @Inject
    protected TestDirectory directory;

    @Inject
    protected RandomSupport random;

    private final RangeLayout layout = new RangeLayout(1);

    @Test
    void shouldAddZeroEntries() throws IOException {
        // given
        try (IndexUpdateStorage<RangeKey> storage = new IndexUpdateStorage<>(
                directory.getFileSystem(),
                directory.file("file"),
                heapBufferFactory(0).globalAllocator(),
                1000,
                layout,
                INSTANCE)) {
            // when
            List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected = generateSomeUpdates(0);
            storeAll(storage, expected);

            // then
            verify(expected, storage);
        }
    }

    @Test
    void shouldAddFewEntries() throws IOException {
        // given
        try (IndexUpdateStorage<RangeKey> storage = new IndexUpdateStorage<>(
                directory.getFileSystem(),
                directory.file("file"),
                heapBufferFactory(0).globalAllocator(),
                1000,
                layout,
                INSTANCE)) {
            // when
            List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected = generateSomeUpdates(5);
            storeAll(storage, expected);

            // then
            verify(expected, storage);
        }
    }

    @Test
    void shouldAddManyEntries() throws IOException {
        // given
        try (IndexUpdateStorage<RangeKey> storage = new IndexUpdateStorage<>(
                directory.getFileSystem(),
                directory.file("file"),
                heapBufferFactory(0).globalAllocator(),
                10_000,
                layout,
                INSTANCE)) {
            // when
            List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected = generateSomeUpdates(1_000);
            storeAll(storage, expected);

            // then
            verify(expected, storage);
        }
    }

    private static void storeAll(
            IndexUpdateStorage<RangeKey> storage, List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected)
            throws IOException {
        for (IndexEntryUpdate<SchemaDescriptorSupplier> update : expected) {
            storage.add(update);
        }
        storage.doneAdding();
    }

    private static void verify(
            List<IndexEntryUpdate<SchemaDescriptorSupplier>> expected, IndexUpdateStorage<RangeKey> storage)
            throws IOException {
        try (IndexUpdateCursor<RangeKey, NullValue> reader = storage.reader()) {
            for (IndexEntryUpdate<SchemaDescriptorSupplier> expectedUpdate : expected) {
                assertTrue(reader.next());
                assertEquals(expectedUpdate, asUpdate(reader));
            }
            assertFalse(reader.next());
        }
    }

    private static IndexEntryUpdate<SchemaDescriptorSupplier> asUpdate(IndexUpdateCursor<RangeKey, NullValue> reader) {
        return switch (reader.updateMode()) {
            case ADDED -> IndexEntryUpdate.add(
                    reader.key().getEntityId(), descriptor, reader.key().asValue());
            case CHANGED -> IndexEntryUpdate.change(
                    reader.key().getEntityId(),
                    descriptor,
                    reader.key().asValue(),
                    reader.key2().asValue());
            case REMOVED -> IndexEntryUpdate.remove(
                    reader.key().getEntityId(), descriptor, reader.key().asValue());
        };
    }

    private List<IndexEntryUpdate<SchemaDescriptorSupplier>> generateSomeUpdates(int count) {
        List<IndexEntryUpdate<SchemaDescriptorSupplier>> updates = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long entityId = random.nextLong(10_000_000);
            switch (random.among(UpdateMode.MODES)) {
                case ADDED -> updates.add(IndexEntryUpdate.add(entityId, descriptor, random.nextValue()));
                case REMOVED -> updates.add(IndexEntryUpdate.remove(entityId, descriptor, random.nextValue()));
                case CHANGED -> updates.add(
                        IndexEntryUpdate.change(entityId, descriptor, random.nextValue(), random.nextValue()));
                default -> throw new IllegalArgumentException();
            }
        }
        return updates;
    }
}
