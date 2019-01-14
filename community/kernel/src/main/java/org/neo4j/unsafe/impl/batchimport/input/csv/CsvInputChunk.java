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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.IOException;

import org.neo4j.csv.reader.Chunker;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

/**
 * {@link InputChunk} that gets data from {@link Chunker}. Making it explicit in the interface simplifies implementation
 * where there are different types of {@link Chunker} for different scenarios.
 */
public interface CsvInputChunk extends InputChunk
{
    /**
     * Fills this {@link InputChunk} from the given {@link Chunker}.
     *
     * @param chunker to read next chunk from.
     * @return {@code true} if there was data read, otherwise {@code false}, meaning end of stream.
     * @throws IOException on I/O read error.
     */
    boolean fillFrom( Chunker chunker ) throws IOException;
}
