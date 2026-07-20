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
package org.neo4j.internal.kernel.api;

/**
 * Cursor for scanning the nodes of a schema index.
 */
public interface NodeIndexCursor extends NodeCursor, EntityIndexCursor {
    void node(NodeCursor cursor);

    @Override
    long nodeReference();

    @Override
    default long reference() {
        return nodeReference();
    }

    /**
     * Reads the node, the one that was most recently read from the index via a successful call to
     * {@link #next()} from the store. After a successful call to this method all the {@link NodeCursor}
     * methods can be used on this cursor. Reading the node from store isn't automatically done
     * in {@link #next()} since it's not always desired to read that data, e.g. if the client only wants
     * the node reference and the property values from the index, if the index supports such.
     *
     * @return {@code true} if the current node from the most recent call to {@link #next()} was
     * successfully read from store.
     */
    boolean readFromStore();
}
