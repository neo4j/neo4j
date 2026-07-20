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
package org.neo4j.collection;

import java.lang.invoke.VarHandle;
import org.neo4j.internal.helpers.VarHandleUtils;

/**
 * A bloom filter {@link <a href="https://en.wikipedia.org/wiki/Bloom_filter">Bloom filters</a>}
 * implementation with the core bitmap held in a {@code long[]}.
 * Concurrent access/update to the filter is supported.
 *
 * This implementation does not impose a serialization order between callers.
 *
 * If there is a serialization order between threads T1 and T2 operations, then:
 * T1: add(id)
 * T2: mayContain(id) -> true
 *
 * Each of the hash bits for an entry is updated as a separate compare-and-set,
 * i.e. there is no "transactional" update of all the bits,
 * but this does not violate the contract of the bloom filter.
 */
public final class ConcurrentLongBloomFilter implements BloomFilter {
    private final int filterSize;
    private final int numHashes;

    private final long[] bloom;

    public ConcurrentLongBloomFilter(int filterSize, int numHashes) {
        this.numHashes = numHashes;
        this.filterSize = filterSize;
        this.bloom = new long[filterSize];
    }

    // Murmurhash3
    private static long hash64(long x) {
        x += 5653741133630908297L;
        x = (x ^ (x >>> 33)) * 0xff51afd7ed558ccdL;
        x = (x ^ (x >>> 33)) * 0xc4ceb9fe1a85ec53L;
        x = x ^ (x >>> 33);
        return x;
    }

    private static final VarHandle LONG_ARRAY = VarHandleUtils.arrayElementVarHandle(long[].class);

    @Override
    public void add(long id) {
        long hash = hash64(id);
        long a = (hash >>> 32) | (hash << 32);
        for (int i = 0; i < numHashes; i++) {
            int index = reduce((int) (a >>> 32));
            long ignore = (long) LONG_ARRAY.getAndBitwiseOr(bloom, index, 1L << a);
            a += hash;
        }
    }

    @Override
    public boolean mayContain(long id) {
        long hash = hash64(id);
        long a = (hash >>> 32) | (hash << 32);
        for (int i = 0; i < numHashes; i++) {
            if (((long) LONG_ARRAY.getVolatile(bloom, reduce((int) (a >>> 32))) & 1L << a) == 0) {
                return false;
            }
            a += hash;
        }
        return true;
    }

    private int reduce(int hash) {
        return (int) (((hash & 0xffffffffL) * filterSize) >>> 32); // fast mod FILTER_SIZE
    }
}
