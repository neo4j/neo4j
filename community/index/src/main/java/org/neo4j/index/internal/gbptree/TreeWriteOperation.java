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
import java.util.function.LongConsumer;
import org.neo4j.index.internal.gbptree.StructurePropagation.StructureUpdate;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Custom tree write operation that can be run by {@link Writer#execute(TreeWriteOperation)}
 * <p>
 * Care should be taken when creating custom write operation. It should work well together with other operations
 * and adhere structure and concurrency coordination practices.
 * <p>
 * Those practices are:
 * 1. Create successors if needed before updating node, using
 * {@link InternalAccess#createSuccessorIfNeeded(PageCursor, StructurePropagation, StructureUpdate, long, long)}
 * 2. Call {@link InternalAccess#handleStructureChanges(PageCursor, StructurePropagation, long, long, CursorContext)}
 * if {@link StructurePropagation} was updated, i.e. by creating successor
 * 3. Consult with {@link TreeWriterCoordination} before performing operations that require coordination
 *
 * @param <K> key
 * @param <V> value
 */
public interface TreeWriteOperation<K, V> {

    /**
     * @return true if operation was successful, if returns false, operation will be retried in pessimistic mode
     */
    boolean run(
            Layout<K, V> layout,
            InternalAccess<K, V> internalAccess,
            PageCursor cursor,
            StructurePropagation<K> structurePropagation,
            long stableGeneration,
            long unstableGeneration,
            CursorContext cursorContext,
            LongConsumer rootSetter,
            IdProvider freeList)
            throws IOException;
}
