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
package org.neo4j.kernel.api.index;

import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.IndexFileSnapshotter;

/**
 * Minimal index accessor used for dropping failed indexes and provide index configuration.
 */
public interface MinimalIndexAccessor extends IndexConfigProvider, IndexFileSnapshotter {
    MinimalIndexAccessor EMPTY = new MinimalIndexAccessor() {
        @Override
        public void drop() {}

        @Override
        public ResourceIterator<Path> snapshotFiles() {
            return emptyResourceIterator();
        }
    };

    /**
     * Deletes this index as well as closes all used external resources.
     * There will not be any interactions after this call.
     *
     * @throws UncheckedIOException if unable to drop index.
     */
    void drop();
}
