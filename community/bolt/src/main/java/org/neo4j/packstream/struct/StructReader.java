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
package org.neo4j.packstream.struct;

import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;

/**
 * Provides necessary logic to decode a given struct type.
 *
 * @param <CTX> a context type.
 * @param <O> a struct POJO type.
 */
public interface StructReader<CTX, O> {

    /**
     * Retrieves the tag with which the struct provided by this reader is typically identified.
     *
     * @return a tag.
     */
    short getTag();

    /**
     * Retrieves the struct from a given buffer based on a previously retrieved header.
     *
     * @param ctx a context object.
     * @param buffer a buffer.
     * @param header a struct header.
     * @return a decoded structure.
     * @throws PackstreamReaderException when decoding the structure fails.
     */
    O read(CTX ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException;
}
