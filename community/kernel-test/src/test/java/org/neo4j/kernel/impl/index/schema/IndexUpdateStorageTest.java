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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;

@TestDirectoryExtension
@RandomSupportExtension
class IndexUpdateStorageTest {
    private static final IndexDescriptor descriptor = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 1))
            .withName("1")
            .materialise(23);

    @Inject
    protected TestDirectory directory;

    @Inject
    protected RandomSupport random;

    private final RangeLayout layout = new RangeLayout(1);

    @Test
    void shouldAddZeroEntries() throws IOException {
        int blockSize = 1000;
        random.withConfiguration(RandomValues.newConfigurationBuilder()
                        .maxVectorNumBytes(blockSize)
                        .build())
                .reset();
        // given
        try (IndexUpdateStorage<RangeKey> storage = new IndexUpdateStorage<>(
                directory.getFileSystem(),
                directory.file("file"),
                heapBufferFactory(0).globalAllocator(),
                blockSize,
                layout,
                INSTANCE)) {
            // when
            List<UpdateInstruction> expected = generateSomeUpdates(0);
            storeAll(storage, expected);

            // then
            verify(expected, storage);
        }
    }

    @Test
    void shouldAddFewEntries() throws IOException {
        int blockSize = 1000;
        int numEntries = 5;
        random.withConfiguration(RandomValues.newConfigurationBuilder()
                        .maxVectorNumBytes(blockSize / numEntries)
                        .build())
                .reset();
        // given
        try (IndexUpdateStorage<RangeKey> storage = new IndexUpdateStorage<>(
                directory.getFileSystem(),
                directory.file("file"),
                heapBufferFactory(0).globalAllocator(),
                blockSize,
                layout,
                INSTANCE)) {
            // when
            List<UpdateInstruction> expected = generateSomeUpdates(numEntries);
            storeAll(storage, expected);

            // then
            verify(expected, storage);
        }
    }

    @Test
    void shouldAddManyEntries() throws IOException {
        int blockSize = 10_000;
        int numEntries = 1_000;
        random.withConfiguration(RandomValues.newConfigurationBuilder()
                        .maxVectorNumBytes(blockSize / numEntries)
                        .build())
                .reset();
        // given
        try (IndexUpdateStorage<RangeKey> storage = new IndexUpdateStorage<>(
                directory.getFileSystem(),
                directory.file("file"),
                heapBufferFactory(0).globalAllocator(),
                blockSize,
                layout,
                INSTANCE)) {
            // when
            List<UpdateInstruction> expected = generateSomeUpdates(numEntries);
            storeAll(storage, expected);

            // then
            verify(expected, storage);
        }
    }

    private static void storeAll(IndexUpdateStorage<RangeKey> storage, List<UpdateInstruction> expected)
            throws IOException {
        for (UpdateInstruction update : expected) {
            storage.add(update.addition, update.key, update.version);
        }
        storage.doneAdding();
    }

    private void verify(List<UpdateInstruction> expected, IndexUpdateStorage<RangeKey> storage) throws IOException {
        try (IndexUpdateCursor<RangeKey> reader = storage.reader()) {
            for (UpdateInstruction expectedUpdate : expected) {
                assertTrue(reader.next());
                Assertions.assertThat(reader.addition()).isEqualTo(expectedUpdate.addition);
                Assertions.assertThat(reader.version()).isEqualTo(expectedUpdate.version);
                Assertions.assertThat(reader.key()).usingComparator(layout).isEqualTo(expectedUpdate.key);
            }
            assertFalse(reader.next());
        }
    }

    private List<UpdateInstruction> generateSomeUpdates(int count) {
        List<UpdateInstruction> updates = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long entityId = random.nextLong(10_000_000);
            RangeKey key = layout.newKey();
            initializeKeyFromUpdate(key, entityId, new Value[] {random.nextValue()});
            long version = random.nextLong(Long.MAX_VALUE);
            switch (random.among(UpdateMode.MODES)) {
                case ADDED -> updates.add(new UpdateInstruction(true, key, version));
                case REMOVED -> updates.add(new UpdateInstruction(false, key, version));
                case CHANGED -> {
                    updates.add(new UpdateInstruction(true, key, version));
                    RangeKey oldKey = layout.newKey();
                    initializeKeyFromUpdate(oldKey, entityId, new Value[] {random.nextValue()});
                    updates.add(new UpdateInstruction(false, oldKey, version));
                }
            }
        }
        return updates;
    }

    private record UpdateInstruction(boolean addition, RangeKey key, long version) {}
}
