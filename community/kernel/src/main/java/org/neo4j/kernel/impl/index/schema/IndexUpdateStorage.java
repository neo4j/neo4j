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

import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

/**
 * Buffer {@link IndexEntryUpdate} by writing them out to a file. Can be read back in insert order through {@link #reader()}.
 */
public class IndexUpdateStorage<KEY extends NativeIndexKey<KEY>>
        extends SimpleEntryStorage<IndexEntryUpdate<?>, IndexUpdateCursor<KEY, NullValue>> {
    private final IndexLayout<KEY> layout;
    private final KEY key1;
    private final KEY key2;
    private final NullValue value = NullValue.INSTANCE;

    IndexUpdateStorage(
            FileSystemAbstraction fs,
            Path file,
            ByteBufferFactory.Allocator byteBufferFactory,
            int blockSize,
            IndexLayout<KEY> layout,
            MemoryTracker memoryTracker) {
        super(fs, file, byteBufferFactory, blockSize, memoryTracker);
        this.layout = layout;
        this.key1 = layout.newKey();
        this.key2 = layout.newKey();
    }

    @Override
    public IndexUpdateCursor<KEY, NullValue> reader(PageCursor pageCursor) {
        return new IndexUpdateCursor<>(pageCursor, layout);
    }

    @Override
    public void add(IndexEntryUpdate<?> update, PageCursor pageCursor) throws IOException {
        ValueIndexEntryUpdate<?> valueUpdate = (ValueIndexEntryUpdate<?>) update;
        final var entrySize = calculateEntrySize(valueUpdate);
        write(pageCursor, valueUpdate.updateMode(), entrySize);
    }

    private int calculateEntrySize(ValueIndexEntryUpdate<?> update) {
        final var entityId = update.getEntityId();
        final var values = update.values();
        final var updateMode = update.updateMode();
        switch (updateMode) {
            case ADDED:
                return TYPE_SIZE + added(key1, entityId, values);
            case CHANGED:
                return TYPE_SIZE + removed(key1, entityId, update.beforeValues()) + added(key2, entityId, values);
            case REMOVED:
                return TYPE_SIZE + removed(key1, entityId, values);
            default:
                throw new IllegalArgumentException("Unknown update mode " + updateMode);
        }
    }

    private int added(KEY key, long entityId, Value[] values) {
        initializeKeyFromUpdate(key, entityId, values);
        return BlockEntry.entrySize(layout, key, value);
    }

    private int removed(KEY key, long entityId, Value[] values) {
        initializeKeyFromUpdate(key, entityId, values);
        return BlockEntry.keySize(layout, key);
    }

    private void write(PageCursor pageCursor, UpdateMode updateMode, int entrySize) throws IOException {
        prepareWrite(entrySize);
        pageCursor.putByte((byte) updateMode.ordinal());
        IndexUpdateEntry.write(pageCursor, layout, updateMode, key1, key2, value);
    }
}
