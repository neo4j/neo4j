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
package org.neo4j.hashing;

import java.nio.ByteBuffer;

/**
 * A hash wide enough to stand in for the bytes it was computed from.
 * <p>
 * Two 64-bit hashes of the same bytes under different seeds will do this, at the price of reading those bytes
 * twice. An implementation of this is expected to read them once.
 */
public interface Hasher128 {
    /**
     * Hashes {@code [off, off + len)} of a buffer to 128 bits, without copying the bytes anywhere first.
     *
     * @param input bytes to hash. Must be {@link java.nio.ByteOrder#LITTLE_ENDIAN}, because the hash reads it
     * eight bytes at a time and the value would otherwise depend on the byte order of a buffer somebody else
     * created.
     * @param off first byte to hash.
     * @param len how many bytes to hash.
     * @param out receives the hash: {@code out[0]} the low half, {@code out[1]} the high half. Supplied by the
     * caller so that hashing allocates nothing; a caller with a thread of its own can keep one array forever.
     */
    void hashBytesTo128(ByteBuffer input, int off, int len, long[] out);
}
