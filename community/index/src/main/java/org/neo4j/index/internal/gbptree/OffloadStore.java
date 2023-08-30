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

import java.io.IOException;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Store used to store dynamic sized keys and values that are too large to be inlined in {@link GBPTree}.
 */
public interface OffloadStore<KEY, VALUE> {
    /**
     * @return max size for entries in this offload store where entry size is keySize + valueSize.
     */
    int maxEntrySize();

    /**
     * Read only key.
     *
     * @see #readKeyValue(long, Object, Object, CursorContext)
     */
    void readKey(long offloadId, KEY into, CursorContext cursorContext) throws IOException;

    /**
     * Read key and value mapped to by given offloadId.
     *
     * @param offloadId id for which to read key and value.
     * @param key instance to read key into.
     * @param value instance to read value into
     * @param cursorContext underlying page cursor context
     * @throws IOException if something went wrong while reading key or value.
     */
    void readKeyValue(long offloadId, KEY key, VALUE value, CursorContext cursorContext) throws IOException;

    /**
     * Read only value.
     *
     * @see #readKeyValue(long, Object, Object, CursorContext)
     */
    void readValue(long offloadId, VALUE into, CursorContext cursorContext) throws IOException;

    /**
     * Store key in offload store.
     *
     * @see #writeKeyValue(Object, Object, long, long, CursorContext)
     */
    long writeKey(KEY key, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException;

    /**
     * Store key and value in offload store, mapping them to offloadId
     * that can be used when reading the key and value back.
     *
     * @param key the key to write to offload store.
     * @param value the value to write to offload store together with key.
     * @param stableGeneration current stable generation when key is written.
     * @param unstableGeneration current unstable generation when key is written.
     * @param cursorContext underlying page cursor context
     * @return offloadId to use when reading key and value back.
     * @throws IOException if something went wrong while writing key or value.
     */
    long writeKeyValue(
            KEY key, VALUE value, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException;

    /**
     * Free the given offloadId effectively deleting that entry from offload store.
     *
     * @param offloadId id to free
     * @param stableGeneration current stable generation when id is freed.
     * @param unstableGeneration current unstable generation when id is freed.
     * @param cursorContext underlying page cursor context
     * @throws IOException if something went wrong when freeing id.
     */
    void free(long offloadId, long stableGeneration, long unstableGeneration, CursorContext cursorContext)
            throws IOException;
}
