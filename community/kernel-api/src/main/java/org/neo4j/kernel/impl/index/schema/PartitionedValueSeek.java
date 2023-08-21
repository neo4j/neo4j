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

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexProgressor;

public interface PartitionedValueSeek {
    /**
     * @return the number of partitions that can be reserved by this instance.
     */
    int getNumberOfPartitions();

    /**
     * @param client the client used for consuming data
     * @param cursorContext The underlying page cursor context for the thread doing the seek.
     * @return An {@link IndexProgressor} used for reading the data of one partition
     * or {@link IndexProgressor#EMPTY} if there are no more partitions.
     */
    IndexProgressor reservePartition(IndexProgressor.EntityValueClient client, CursorContext cursorContext);
}
