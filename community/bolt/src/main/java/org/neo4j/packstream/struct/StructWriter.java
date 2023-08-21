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

import org.neo4j.packstream.io.PackstreamBuf;

/**
 * Provides logic to encode structs to their wire format as well as related metadata.
 *
 * @param <CTX> a context type.
 * @param <I> a struct POJO type.
 */
public interface StructWriter<CTX, I> {

    /**
     * Identifies the preferred type returned by this writer.
     *
     * @return a writer implementation.
     */
    Class<I> getType();

    /**
     * Retrieves the tag which uniquely identifies this particular struct type within a data stream.
     *
     * @param payload a payload.
     * @return a struct tag.
     */
    short getTag(I payload);

    /**
     * Retrieves the total amount of fields within this struct for a given payload.
     *
     * @param payload a payload.
     * @return the number of fields within the payload.
     */
    long getLength(I payload);

    /**
     * Writes a struct payload to the given buffer.
     *
     * @param ctx a context object.
     * @param buffer  a buffer.
     * @param payload a struct payload.
     */
    void write(CTX ctx, PackstreamBuf buffer, I payload);
}
