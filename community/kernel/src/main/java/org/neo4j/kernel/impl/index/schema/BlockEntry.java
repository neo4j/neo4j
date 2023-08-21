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

import static java.lang.String.format;
import static org.neo4j.util.Preconditions.checkState;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * A {@link BlockEntry} is a key-value mapping and the smallest unit in the {@link BlockStorage} and {@link IndexUpdateStorage} hierarchy. Except for being a
 * container class for key-value pairs, it also provide static methods for serializing and deserializing {@link BlockEntry} instances and calculating total
 * store size of them.
 */
record BlockEntry<KEY, VALUE>(KEY key, VALUE value) {

    private static final int ENTRY_OVERHEAD = Short.BYTES * 2;

    @Override
    public String toString() {
        return format("[%s=%s]", key, value);
    }

    static <VALUE, KEY> int entrySize(Layout<KEY, VALUE> layout, KEY key, VALUE value) {
        int keySize = layout.keySize(key);
        int valueSize = layout.valueSize(value);
        return keySize + valueSize + ENTRY_OVERHEAD;
    }

    static <VALUE, KEY> int keySize(Layout<KEY, VALUE> layout, KEY key) {
        int keySize = layout.keySize(key);
        return keySize + ENTRY_OVERHEAD;
    }

    static <KEY, VALUE> BlockEntry<KEY, VALUE> read(PageCursor pageCursor, Layout<KEY, VALUE> layout) {
        KEY key = layout.newKey();
        VALUE value = layout.newValue();
        read(pageCursor, layout, key, value);
        return new BlockEntry<>(key, value);
    }

    static <KEY, VALUE> void read(PageCursor pageCursor, Layout<KEY, VALUE> layout, KEY key, VALUE value) {
        var keySize = pageCursor.getShort();
        var valueSize = pageCursor.getShort();
        layout.readKey(pageCursor, key, keySize);
        layout.readValue(pageCursor, value, valueSize);
    }

    static <KEY, VALUE> void read(PageCursor pageCursor, Layout<KEY, VALUE> layout, KEY key) {
        var keySize = pageCursor.getShort();
        var valueSize = pageCursor.getShort();
        checkState(valueSize == 0, "Expected 0 value size");
        layout.readKey(pageCursor, key, keySize);
    }

    static <KEY, VALUE> void write(PageCursor pageCursor, Layout<KEY, VALUE> layout, KEY key, VALUE value) {
        int keySize = layout.keySize(key);
        int valueSize = layout.valueSize(value);
        checkState(((short) keySize) == keySize, "Key size overflow");
        checkState(((short) valueSize) == valueSize, "Value size overflow");
        pageCursor.putShort((short) keySize);
        pageCursor.putShort((short) valueSize);
        layout.writeKey(pageCursor, key);
        layout.writeValue(pageCursor, value);
    }

    static <KEY, VALUE> void write(PageCursor pageCursor, Layout<KEY, VALUE> layout, KEY key) {
        int keySize = layout.keySize(key);
        checkState(((short) keySize) == keySize, "Key size overflow");
        pageCursor.putShort((short) keySize);
        pageCursor.putShort((short) 0);
        layout.writeKey(pageCursor, key);
    }
}
