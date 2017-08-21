/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.hashing;

/**
 * The (our) incremental XXH64 hash function.
 * <p>
 * This hash function is based on xxHash (XXH64 variant), but modified to work incrementally on 8-byte blocks
 * instead of on 32-byte blocks. Basically, the 32-byte block hash loop has been removed, so we use the 8-byte
 * block tail-loop for the entire input.
 * <p>
 * This hash function is roughly twice as fast as its predecessor, about 30% faster than optimised murmurhash3
 * implementations though not as fast as optimised xxHash implementations due to the smaller block size.
 * It is allocation free, unlike its predecessor. And it retains most of the excellent statistical properties of
 * xxHash, failing only the "TwoBytes" and "Zeroes" keyset tests in SMHasher, passing 12 out of 14 tests.
 * <p>
 * The hasher is used by first {@link IncrementalXXH64#init(long) initialising} the state with a seed, which can be any
 * arbitrary 64-bit integer, including zero. The produced state is then recursively cycled through the
 * {@link IncrementalXXH64#update(long, long) update} function and merged with an input word as many times as desired.
 * When the input has been consumed, the final hash value is then produced from the last intermediate hash value with
 * the {@link IncrementalXXH64#finalise(long) finalise} function.
 * <p>
 * The xxHash algorithm is originally by Yann Collet, http://cyan4973.github.io/xxHash/, and this implementation is
 * with inspiration from Vsevolod Tolstopyatovs implementation in https://github.com/OpenHFT/Zero-Allocation-Hashing.
 * Credit for SMHasher goes to Austin Appleby, https://github.com/aappleby/smhasher.
 */
public class IncrementalXXH64
{
    private static final long Prime1 = -7046029288634856825L;
    private static final long Prime2 = -4417276706812531889L;
    private static final long Prime3 = 1609587929392839161L;
    private static final long Prime4 = -8796714831421723037L;
    private static final long Prime5 = 2870177450012600261L;

    private IncrementalXXH64()
    {
    }

    public static long init( long seed )
    {
        return seed + Prime5;
    }

    public static long update( long hash, long block )
    {
        hash += 8;
        block *= Prime2;
        block = Long.rotateLeft( block, 31 );
        block *= Prime1;
        hash ^= block;
        hash = Long.rotateLeft( hash, 27 ) * Prime1 + Prime4;
        return hash;
    }

    public static long finalise( long hash )
    {
        hash ^= hash >>> 33;
        hash *= Prime2;
        hash ^= hash >>> 29;
        hash *= Prime3;
        hash ^= hash >>> 32;
        return hash;
    }
}
