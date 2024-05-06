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
package org.neo4j.internal.id.indexed;

import static java.lang.String.format;

import java.nio.ByteBuffer;
import org.neo4j.index.internal.gbptree.Header;

/**
 * {@link Header.Reader} capable of reading header of an {@link IndexedIdGenerator}. After read correctly the header data
 * will exist as fields in the instance.
 *
 * @see HeaderWriter
 */
public class HeaderReader implements Header.Reader {
    static final long UNINITIALIZED = -1;

    boolean wasRead;
    long highId;
    long highestWrittenId;
    long generation;
    int idsPerEntry;
    long numUnusedIds;

    @Override
    public void read(ByteBuffer headerBytes) {
        this.wasRead = true;
        this.highId = headerBytes.getLong();
        this.highestWrittenId = headerBytes.getLong();
        this.generation = headerBytes.getLong();
        this.idsPerEntry = headerBytes.getInt();
        this.numUnusedIds = headerBytes.hasRemaining() ? headerBytes.getLong() : UNINITIALIZED;
    }

    @Override
    public String toString() {
        return format("High-ID:%d, Highest-ID written:%d, Generation:%d", highId, highestWrittenId, generation);
    }
}
