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
package org.neo4j.batchimport.api;

import java.io.Closeable;
import java.io.IOException;
import org.neo4j.batchimport.api.input.InputChunk;

/**
 * WARNING: Implementations must be thread safe
 */
public interface InputIterator extends Closeable {
    /**
     * Called by each thread that will be reading input data and the returned instances will be local to that thread.
     * @return an instance which is capable of receiving data in chunks when passed into {@link #next(InputChunk)}.
     */
    InputChunk newChunk();

    /**
     * Fills the given {@code chunk} with more data. Should be called by the same thread that {@link #newChunk() allocated} the chunk.
     * @param chunk to receive the new data.
     * @return {@code true} if data was retreived into the chunk, otherwise {@code false} if there was no more data to be read.
     * @throws IOException on I/O error.
     */
    boolean next(InputChunk chunk) throws IOException;
}
