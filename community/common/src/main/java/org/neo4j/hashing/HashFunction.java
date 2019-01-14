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
package org.neo4j.hashing;

/**
 * A hash function, as per this interface, will produce a deterministic value based on its input.
 * <p>
 * Hash functions are first initialised with a seed, which may be zero, and then updated with a succession of values
 * that are mixed into the hash state in sequence.
 * <p>
 * Hash functions may have internal state, but can also be stateless, if their complete state can be represented by the
 * 64-bit intermediate hash state.
 *
 * @see #incrementalXXH64()
 * @see #javaUtilHashing()
 * @see #xorShift32()
 */
public interface HashFunction
{
    /**
     * Initialise the hash function with the given seed.
     * <p>
     * Different seeds should produce different final hash values.
     *
     * @param seed The initialisation seed for the hash function.
     * @return An initialised intermediate hash state.
     */
    long initialise( long seed );

    /**
     * Update the hash state by mixing the given value into the given intermediate hash state.
     *
     * @param intermediateHash The intermediate hash state given either by {@link #initialise(long)}, or by a previous
     * call to this function.
     * @param value The value to add to the hash state.
     * @return a new intermediate hash state with the value mixed in.
     */
    long update( long intermediateHash, long value );

    /**
     * Produce a final hash value from the given intermediate hash state.
     *
     * @param intermediateHash the intermediate hash state from which to produce a final hash value.
     * @return the final hash value.
     */
    long finalise( long intermediateHash );

    /**
     * Reduce the given 64-bit hash value to a 32-bit value.
     *
     * @param hash The hash value to reduce.
     * @return The 32-bit representation of the given hash value.
     */
    default int toInt( long hash )
    {
        return (int) ((hash >> 32) ^ hash);
    }

    /**
     * Produce a 64-bit hash value from a single long value.
     *
     * @param value The single value to hash.
     * @return The hash of the given value.
     */
    default long hashSingleValue( long value )
    {
        return finalise( update( initialise( 0 ), value ) );
    }

    /**
     * Produce a 32-bit hash value from a single long value.
     *
     * @param value The single value to hash.
     * @return The hash of the given value.
     */
    default int hashSingleValueToInt( long value )
    {
        return toInt( hashSingleValue( value ) );
    }

    /**
     * Our incremental XXH64 based hash function.
     * <p>
     * This hash function is based on xxHash (XXH64 variant), but modified to work incrementally on 8-byte blocks
     * instead of on 32-byte blocks. Basically, the 32-byte block hash loop has been removed, so we use the 8-byte
     * block tail-loop for the entire input.
     * <p>
     * This hash function is roughly twice as fast as the hash function used for index entries since 2.2.0, about 30%
     * faster than optimised murmurhash3 implementations though not as fast as optimised xxHash implementations due to
     * the smaller block size. It is allocation free, unlike its predecessor. And it retains most of the excellent
     * statistical properties of xxHash, failing only the "TwoBytes" and "Zeroes" keyset tests in SMHasher, passing 12
     * out of 14 tests. According to <a href="https://twitter.com/Cyan4973/status/899995095549698049">Yann Collet on
     * twitter</a>, this modification is expected to mostly cause degraded performance, and worsens some of the
     * avalanche statistics.
     * <p>
     * This hash function is stateless, so the returned instance can be freely cached and accessed concurrently by
     * multiple threads.
     * <p>
     * The <a href="http://cyan4973.github.io/xxHash/">xxHash</a> algorithm is originally by Yann Collet, and this
     * implementation is with inspiration from Vsevolod Tolstopyatovs implementation in the
     * <a href="https://github.com/OpenHFT/Zero-Allocation-Hashing">Zero Allocation Hashing</a> library.
     * Credit for <a href="https://github.com/aappleby/smhasher">SMHasher</a> goes to Austin Appleby.
     */
    static HashFunction incrementalXXH64()
    {
        return IncrementalXXH64.INSTANCE;
    }

    /**
     * Same hash function as that used by the standard library hash collections. It generates a hash by splitting the
     * input value into segments, and then re-distributing those segments, so the end result is effectively a striped
     * and then jumbled version of the input data. For randomly distributed keys, this has a good chance at generating
     * an even hash distribution over the full hash space.
     * <p>
     * It performs exceptionally poorly for sequences of numbers, as the sequence increments all end up in the same
     * stripe, generating hash values that will end up in the same buckets in collections.
     * <p>
     * This hash function is stateless, so the returned instance can be freely cached and accessed concurrently by
     * multiple threads.
     */
    static HashFunction javaUtilHashing()
    {
        return JavaUtilHashFunction.INSTANCE;
    }

    /**
     * The default hash function is based on a pseudo-random number generator, which uses the input value as a seed
     * to the generator. This is very fast, and performs well for most input data. However, it is not guaranteed to
     * generate a superb distribution, only a "decent" one.
     * <p>
     * This hash function is stateless, so the returned instance can be freely cached and accessed concurrently by
     * multiple threads.
     */
    static HashFunction xorShift32()
    {
        return XorShift32HashFunction.INSTANCE;
    }
}
