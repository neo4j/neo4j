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
package org.neo4j.index.internal.gbptree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Default {@link Layout} for the "root layer" tree in a multi-root {@link MultiRootGBPTree}, containing mappings to all data trees.
 */
class RootMappingLayout<ROOT_KEY> extends Layout.Adapter<ROOT_KEY, RootMappingLayout.RootMappingValue> {
    private static final long IDENTIFIER = 53468735487453L;
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 1;
    private static final int KEY_LAYOUT_VERSION_SHIFT = Integer.SIZE / 2;

    private final KeyLayout<ROOT_KEY> keyLayout;

    protected RootMappingLayout(KeyLayout<ROOT_KEY> keyLayout) {
        super(
                keyLayout.fixedSize(),
                keyLayout.identifier() ^ IDENTIFIER,
                keyLayout.majorVersion() << KEY_LAYOUT_VERSION_SHIFT | MAJOR_VERSION,
                keyLayout.minorVersion() << KEY_LAYOUT_VERSION_SHIFT | MINOR_VERSION);
        this.keyLayout = keyLayout;
    }

    @Override
    public ROOT_KEY newKey() {
        return keyLayout.newKey();
    }

    @Override
    public ROOT_KEY copyKey(ROOT_KEY key, ROOT_KEY into) {
        return keyLayout.copyKey(key, into);
    }

    @Override
    public RootMappingValue newValue() {
        return new RootMappingValue();
    }

    @Override
    public int keySize(ROOT_KEY key) {
        return keyLayout.keySize(key);
    }

    @Override
    public int valueSize(RootMappingValue value) {
        return Long.BYTES * 2;
    }

    @Override
    public void writeKey(PageCursor cursor, ROOT_KEY key) {
        keyLayout.writeKey(cursor, key);
    }

    @Override
    public void writeValue(PageCursor cursor, RootMappingValue value) {
        cursor.putLong(value.rootId);
        cursor.putLong(value.rootGeneration);
    }

    @Override
    public void readKey(PageCursor cursor, ROOT_KEY into, int keySize) {
        keyLayout.readKey(cursor, into, keySize);
    }

    @Override
    public void readValue(PageCursor cursor, RootMappingValue into, int valueSize) {
        into.rootId = cursor.getLong();
        into.rootGeneration = cursor.getLong();
    }

    @Override
    public void initializeAsLowest(ROOT_KEY key) {
        keyLayout.initializeAsLowest(key);
    }

    @Override
    public void initializeAsHighest(ROOT_KEY key) {
        keyLayout.initializeAsHighest(key);
    }

    @Override
    public int compare(ROOT_KEY o1, ROOT_KEY o2) {
        return keyLayout.compare(o1, o2);
    }

    static class RootMappingValue {
        long rootId;
        long rootGeneration;

        RootMappingValue initialize(Root root) {
            this.rootId = root.id();
            this.rootGeneration = root.generation();
            return this;
        }

        Root asRoot() {
            return new Root(rootId, rootGeneration);
        }
    }
}
