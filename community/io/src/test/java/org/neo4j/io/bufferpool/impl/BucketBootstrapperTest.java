/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.io.bufferpool.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.EmptyMemoryTracker;

class BucketBootstrapperTest {

    @Test
    void testDefaultBucketCapacitiesWith2Cpus() {
        var bootstrapper = new TestBucketBootstrapper2();
        var buckets = bootstrapper.getBuckets();
        assertCapacities(
                buckets, 256, 512, kb(1), kb(2), kb(4), kb(8), 16896, kb(32), kb(64), kb(128), kb(256), kb(512),
                kb(1024));

        assertSlices(buckets, 256, 1);
        assertSlices(buckets, 512, 1);
        assertSlices(buckets, kb(1), 1);
        assertSlices(buckets, kb(2), 1);
        assertSlices(buckets, kb(4), 1);
        assertSlices(buckets, kb(8), 1);
        assertSlices(buckets, 16896, 1);
        assertSlices(buckets, kb(32), 1);
        assertSlices(buckets, kb(64), 1);
        assertSlices(buckets, kb(128), 1);
        assertSlices(buckets, kb(256), 1);
        assertSlices(buckets, kb(512), 1);
        assertSlices(buckets, kb(1024), 1);
    }

    @Test
    void testDefaultBucketCapacitiesWith16Cpus() {
        var bootstrapper = new TestBucketBootstrapper16();
        var buckets = bootstrapper.getBuckets();
        assertCapacities(
                buckets, 256, 512, kb(1), kb(2), kb(4), kb(8), 16896, kb(32), kb(64), kb(128), kb(256), kb(512),
                kb(1024));

        assertSlices(buckets, 256, 2);
        assertSlices(buckets, 512, 2);
        assertSlices(buckets, kb(1), 2);
        assertSlices(buckets, kb(2), 2);
        assertSlices(buckets, kb(4), 2);
        assertSlices(buckets, kb(8), 2);
        assertSlices(buckets, 16896, 2);
        assertSlices(buckets, kb(32), 2);
        assertSlices(buckets, kb(64), 2);
        assertSlices(buckets, kb(128), 1);
        assertSlices(buckets, kb(256), 1);
        assertSlices(buckets, kb(512), 1);
        assertSlices(buckets, kb(1024), 1);
    }

    @Test
    void testDefaultBucketCapacitiesWith40Cpus() {
        var bootstrapper = new TestBucketBootstrapper40();
        var buckets = bootstrapper.getBuckets();
        assertCapacities(
                buckets, 256, 512, kb(1), kb(2), kb(4), kb(8), 16896, kb(32), kb(64), kb(128), kb(256), kb(512),
                kb(1024));

        assertSlices(buckets, 256, 5);
        assertSlices(buckets, 512, 5);
        assertSlices(buckets, kb(1), 5);
        assertSlices(buckets, kb(2), 5);
        assertSlices(buckets, kb(4), 5);
        assertSlices(buckets, kb(8), 5);
        assertSlices(buckets, 16896, 5);
        assertSlices(buckets, kb(32), 5);
        assertSlices(buckets, kb(64), 5);
        assertSlices(buckets, kb(128), 1);
        assertSlices(buckets, kb(256), 1);
        assertSlices(buckets, kb(512), 1);
        assertSlices(buckets, kb(1024), 1);
    }

    private static int kb(int value) {
        return (int) ByteUnit.kibiBytes(value);
    }

    private static void assertSlices(List<Bucket> buckets, int bufferCapacity, int expectedSliceCount) {
        var sliceCount = buckets.stream()
                .filter(bucket -> bucket.getBufferCapacity() == bufferCapacity)
                .map(bucket -> bucket.getSlices().size())
                .findFirst()
                .get();
        assertEquals(expectedSliceCount, sliceCount);
    }

    private static void assertCapacities(List<Bucket> buckets, Integer... capacities) {
        var bucketCapacities = buckets.stream().map(Bucket::getBufferCapacity).collect(Collectors.toList());
        assertEquals(bucketCapacities, Arrays.asList(capacities));
    }

    private abstract static class TestBucketBootstrapper extends BucketBootstrapper {
        TestBucketBootstrapper() {
            super(EmptyMemoryTracker.INSTANCE);
        }
    }

    private static class TestBucketBootstrapper2 extends TestBucketBootstrapper {
        @Override
        protected int getAvailableCpuCount() {
            return 2;
        }
    }

    private static class TestBucketBootstrapper16 extends TestBucketBootstrapper {
        @Override
        protected int getAvailableCpuCount() {
            return 16;
        }
    }

    private static class TestBucketBootstrapper40 extends TestBucketBootstrapper {
        @Override
        protected int getAvailableCpuCount() {
            return 40;
        }
    }
}
