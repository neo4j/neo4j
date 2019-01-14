/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.csv.reader;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.csv.reader.Source.Chunk;

/**
 * Takes a bigger stream of data and chunks it up into smaller chunks. The {@link Chunk chunks} are allocated
 * explicitly and are passed into {@link #nextChunk(Chunk)} to be filled/assigned with data representing
 * next chunk from the stream. This design allows for efficient reuse of chunks when there are multiple concurrent
 * processors, each processing chunks of data.
 */
public interface Chunker extends Closeable
{
    /**
     * @return a new allocated {@link Chunk} which is to be later passed into {@link #nextChunk(Chunk)}
     * to fill it with data. When a {@link Chunk} has been fully processed then it can be passed into
     * {@link #nextChunk(Chunk)} again to get more data.
     */
    Chunk newChunk();

    /**
     * Fills a previously {@link #newChunk() allocated chunk} with data to be processed after completion
     * of this call.
     *
     * @param chunk {@link Chunk} to fill with data.
     * @return {@code true} if at least some amount of data was passed into the given {@link Chunk},
     * otherwise {@code false} denoting the end of the stream.
     * @throws IOException on I/O error.
     */
    boolean nextChunk( Chunk chunk ) throws IOException;

    /**
     * @return byte position of how much data has been returned from {@link #nextChunk(Chunk)}.
     */
    long position();
}
