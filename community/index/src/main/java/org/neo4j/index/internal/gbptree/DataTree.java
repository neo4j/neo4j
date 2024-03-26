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
 * Main access to a single data tree.
 * As an example {@link MultiRootGBPTree} has the ability to create multiple {@link DataTree}, whereas {@link GBPTree} is a specialized
 * variant which always has a single {@link DataTree}.
 */
public interface DataTree<KEY, VALUE> extends Seeker.Factory<KEY, VALUE> {
    int W_BATCHED_SINGLE_THREADED = 0x1;
    int W_SPLIT_KEEP_ALL_LEFT = 0x2;
    int W_SPLIT_KEEP_ALL_RIGHT = 0x4;

    /**
     * Defaults to parallel writer, i.e. no special flags set.
     * @see #writer(int, CursorContext)
     */
    default Writer<KEY, VALUE> writer(CursorContext cursorContext) throws IOException {
        return writer(0, cursorContext);
    }

    /**
     * Returns a {@link Writer} able to modify the tree, i.e. insert and remove keys/values.
     * After usage the returned writer must be closed, typically by using try-with-resource clause.
     *
     * @param flags specifies certain behaviour of the writer. The default is to support parallel writers and splitting nodes in the middle.
     * If {@link #W_BATCHED_SINGLE_THREADED} is provided then the returned writer is the only allowed writer open at this point in time,
     * until it gets closed. Such a writer will also have some optimizations for inserting many entries in ascending key order.
     * @param cursorContext underlying page cursor context
     * @return a {@link Writer} for this tree. The returned writer must be {@link Writer#close() closed} after usage.
     * @throws IllegalStateException for calls made between a successful call to this method and closing the
     * returned writer, iff {@link #W_BATCHED_SINGLE_THREADED} flag was provided and the implementation supports such a writer.
     */
    Writer<KEY, VALUE> writer(int flags, CursorContext cursorContext) throws IOException;

    /**
     * Calculates an estimate of number of keys in this tree in O(log(n)) time. The number is only an estimate and may make its decision on a
     * concurrently changing tree, but should usually be correct within a couple of percents margin.
     * @param cursorContext underlying page cursor context
     *
     * @return an estimate of number of keys in the tree.
     */
    long estimateNumberOfEntriesInTree(CursorContext cursorContext) throws IOException;

    boolean exists(CursorContext cursorContext);
}
