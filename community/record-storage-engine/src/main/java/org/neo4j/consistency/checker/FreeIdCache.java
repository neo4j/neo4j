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
package org.neo4j.consistency.checker;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.util.VisibleForTesting;

class FreeIdCache {
    private static final int MAX_CACHE_SIZE = 100000;
    private static final int BLOOM_FILTER_SIZE_FACTOR = 10; // This will result in a 8MB bloom filter
    private final IdGenerator idGenerator;
    private final int maxItemsInCache;
    private final long highId;
    private volatile LongSet cache;
    private volatile FreeIdsBloomFilter bloomFilter;

    FreeIdCache(IdGenerator idGenerator) {
        this(idGenerator, MAX_CACHE_SIZE);
    }

    @VisibleForTesting
    FreeIdCache(IdGenerator idGenerator, int maxItemsInCache) {
        this.idGenerator = idGenerator;
        this.maxItemsInCache = maxItemsInCache;
        this.highId = idGenerator.getHighId();
    }

    void initialize() {
        buildCache(maxItemsInCache);
    }

    /**
     * Check if an ID is free (up for reuse) in the provided id generator.
     * Safe for concurrent use, as this is a read-only cache.
     * Note: this is only intended for "offline" use
     * @param id The ID to check
     * @return true if it is up for reuse, false otherwise
     */
    boolean isIdFree(long id) {
        assert cache != null || bloomFilter != null : "Must be initialized before use";
        if (id >= highId) {
            return true;
        }
        if (cache != null) {
            return cache.contains(id);
        }
        if (bloomFilter.idMayBeFree(id)) {
            // This is either for inconsistencies or false positives (should be rare)
            // It's not optimal to create a seeker for each check, but it works for now
            try (PrimitiveLongResourceIterator freeId = idGenerator.notUsedIdsIterator(id, id)) {
                return freeId.hasNext() && freeId.next() == id;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return false;
    }

    private void buildCache(int maxItemsInCache) {
        try {
            MutableLongSet ids = LongSets.mutable.empty();
            try (PrimitiveLongResourceIterator freeIdsIterator = idGenerator.notUsedIdsIterator()) {
                // First fill the set
                while (freeIdsIterator.hasNext() && ids.size() < maxItemsInCache) {
                    ids.add(freeIdsIterator.next());
                }
                if (!freeIdsIterator.hasNext()) {
                    cache = ids;
                } else {
                    cache = null;
                    bloomFilter = new FreeIdsBloomFilter(maxItemsInCache * BLOOM_FILTER_SIZE_FACTOR, 7);
                    ids.forEach(bloomFilter::add);
                    while (freeIdsIterator.hasNext()) {
                        bloomFilter.add(freeIdsIterator.next());
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static class FreeIdsBloomFilter {
        private final int filterSize;
        private final int numHashes;

        private final long[] data;

        FreeIdsBloomFilter(int filterSize, int numHashes) {
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

        void add(long id) {
            long hash = hash64(id);
            long a = (hash >>> 32) | (hash << 32);
            for (int i = 0; i < numHashes; i++) {
                data[reduce((int) (a >>> 32))] |= 1L << a;
                a += hash;
            }
        }

        boolean idMayBeFree(long id) {
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
}
