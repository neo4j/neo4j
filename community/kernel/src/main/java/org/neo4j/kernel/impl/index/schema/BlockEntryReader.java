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

import java.io.IOException;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Reads {@link BlockEntry} from a block in sequential order. Key and value instances are handed out though {@link #key()} and {@link #value()} but are reused
 * internally so consumer need to either create a copy or finish all operations on key and value before progressing reader.
 * Reader will figure out when to stop reading based on Block header wish contains total size of this Block in bytes and total number of entries in Block.
 */
public class BlockEntryReader<KEY, VALUE> implements BlockEntryCursor<KEY, VALUE> {
    private final long blockSize;
    private final long entryCount;
    private final PageCursor pageCursor;
    private final boolean produceNewKeyAndValueInstances;
    private final Layout<KEY, VALUE> layout;
    private KEY key;
    private VALUE value;
    private long readEntries;

    /**
     * @param produceNewKeyAndValueInstances whether or not to let each {@link #next()} instantiate new KEY and VALUE instances.
     * If {@code false} the single KEY and VALUE instances will be reused and its data overwritten with each invokation to {@link #next()}.
     */
    BlockEntryReader(PageCursor pageCursor, Layout<KEY, VALUE> layout, boolean produceNewKeyAndValueInstances) {
        this.pageCursor = pageCursor;
        this.blockSize = pageCursor.getLong();
        this.entryCount = pageCursor.getLong();
        this.layout = layout;
        this.key = produceNewKeyAndValueInstances ? null : layout.newKey();
        this.value = produceNewKeyAndValueInstances ? null : layout.newValue();
        this.produceNewKeyAndValueInstances = produceNewKeyAndValueInstances;
    }

    @Override
    public boolean next() throws IOException {
        if (readEntries >= entryCount) {
            return false;
        }
        if (produceNewKeyAndValueInstances) {
            key = layout.newKey();
            value = layout.newValue();
        }
        BlockEntry.read(pageCursor, layout, key, value);
        readEntries++;
        return true;
    }

    public long blockSize() {
        return blockSize;
    }

    public long entryCount() {
        return entryCount;
    }

    @Override
    public KEY key() {
        return key;
    }

    @Override
    public VALUE value() {
        return value;
    }

    @Override
    public void close() throws IOException {
        pageCursor.close();
    }
}
