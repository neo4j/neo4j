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

import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;

public enum WriterFactory {
    SINGLE {
        @Override
        <KEY, VALUE> Writer<KEY, VALUE> create(GBPTree<KEY, VALUE> index, int flags) throws IOException {
            return index.writer(flags | W_BATCHED_SINGLE_THREADED, NULL_CONTEXT);
        }
    },
    PARALLEL {
        @Override
        <KEY, VALUE> Writer<KEY, VALUE> create(GBPTree<KEY, VALUE> index, int flags) throws IOException {
            return index.writer(flags, NULL_CONTEXT);
        }
    };

    abstract <KEY, VALUE> Writer<KEY, VALUE> create(GBPTree<KEY, VALUE> index, int flags) throws IOException;

    <KEY, VALUE> Writer<KEY, VALUE> create(GBPTree<KEY, VALUE> index) throws IOException {
        return create(index, 0);
    }
}
