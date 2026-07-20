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

/**
 * A bloom filter {@link <a href="https://en.wikipedia.org/wiki/Bloom_filter">Bloom filters</a>}
 * implementation with the core bitmap held in a {@code long[]}.
 *
 * This implementation does not support concurrent access.
 */
public class LongBloomFilter implements BloomFilter {
    private final int filterSize;
    private final int numHashes;

    private final long[] data;

    public LongBloomFilter(int filterSize, int numHashes) {
        this.numHashes = numHashes;
        this.filterSize = filterSize;
        data = new long[filterSize];
    }

    private static long hash64(long x) {
        x += 5653741133630908297L;
        x = (x ^ (x >>> 33)) * 0xff51afd7ed558ccdL;
        x = (x ^ (x >>> 33)) * 0xc4ceb9fe1a85ec53L;
        x = x ^ (x >>> 33);
        return x;
    }

    @Override
    public void add(long id) {
        long hash = hash64(id);
        long a = (hash >>> 32) | (hash << 32);
        for (int i = 0; i < numHashes; i++) {
            data[reduce((int) (a >>> 32))] |= 1L << a;
            a += hash;
        }
    }

    @Override
    public boolean mayContain(long id) {
        long hash = hash64(id);
        long a = (hash >>> 32) | (hash << 32);
        for (int i = 0; i < numHashes; i++) {
            if ((data[reduce((int) (a >>> 32))] & 1L << a) == 0) {
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
