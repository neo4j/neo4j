/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProgressor;

/**
 * TokenScans are used for accessing entities with a given token.
 */
public interface TokenScan
{
    /**
     * Initialize the client for scanning for a token.
     *
     * @param client the client used for consuming data
     * @param cursorTracer underlying page cursor tracer
     * @param indexOrder the order in which to obtain results
     * @return a progressor used for reading data
     */
    IndexProgressor initialize( IndexProgressor.EntityTokenClient client, IndexOrder indexOrder, PageCursorTracer cursorTracer );

    /**
     * Initialize the client for reading a batch of tokens.
     *
     * @param client the client used for consuming data
     * @param sizeHint the approximate size of the batch
     * @param cursorTracer underlying page cursor tracer
     * @return an iterator used for reading data
     */
    IndexProgressor initializeBatch( IndexProgressor.EntityTokenClient client, int sizeHint, PageCursorTracer cursorTracer );
}
