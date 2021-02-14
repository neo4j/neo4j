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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.io.ByteUnit;
import org.neo4j.memory.EmptyMemoryTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BucketBootstrapperTest
{
    @Test
    void testBasicBucketCreation()
    {
        var buckets = constructBuckets( "512:1C", "1K:1C" );
        assertCapacities( buckets, 512, 1024 );

        assertSlices( buckets, 512, 16 );
        assertSlices( buckets, 1024, 16 );
    }

    @Test
    void testSliceCalculation()
    {
        var buckets = constructBuckets( "512:0.01C" );
        assertSlices( buckets, 512, 1 );

        buckets = constructBuckets( "512:0.1C" );
        assertSlices( buckets, 512, 2 );

        buckets = constructBuckets( "512:0.125C" );
        assertSlices( buckets, 512, 2 );

        buckets = constructBuckets( "512:0.13C" );
        assertSlices( buckets, 512, 3 );

        buckets = constructBuckets( "512:0.2C" );
        assertSlices( buckets, 512, 4 );
    }

    @Test
    void testDefaultBucketCapacities()
    {
        var buckets = constructBuckets();
        assertCapacities( buckets, 256, 512, kb( 1 ), kb( 2 ), kb( 4 ), kb( 8 ),
                16896, kb( 32 ), kb( 64 ), kb( 128 ), kb( 256 ), kb( 512 ), kb( 1024 ) );

        assertSlices( buckets, 256, 2 );
        assertSlices( buckets, 512, 2 );
        assertSlices( buckets, kb( 1 ), 2 );
        assertSlices( buckets, kb( 2 ), 2 );
        assertSlices( buckets, kb( 4 ), 2 );
        assertSlices( buckets, kb( 8 ), 2 );
        assertSlices( buckets, 16896, 2 );
        assertSlices( buckets, kb( 32 ), 2 );
        assertSlices( buckets, kb( 64 ), 2 );
        assertSlices( buckets, kb( 128 ), 1 );
        assertSlices( buckets, kb( 256 ), 1 );
        assertSlices( buckets, kb( 512 ), 1 );
        assertSlices( buckets, kb( 1024 ), 1 );
    }

    private int kb( int value )
    {
        return (int) ByteUnit.kibiBytes( value );
    }

    private void assertSlices( List<Bucket> buckets, int bufferCapacity, int expectedSliceCount )
    {
        var sliceCount = buckets.stream()
                                .filter( bucket -> bucket.getBufferCapacity() == bufferCapacity )
                                .map( bucket -> bucket.getSlices().size() )
                                .findFirst().get();
        assertEquals( expectedSliceCount, sliceCount );
    }

    private List<Bucket> constructBuckets( String... expressions )
    {
        var config = new NeoBufferPoolConfigOverride( Duration.ZERO, null, Arrays.asList( expressions ) );
        var bootstrapper = new TestBucketBootstrapper( config );
        return bootstrapper.getBuckets();
    }

    private void assertCapacities( List<Bucket> buckets, Integer... capacities )
    {
        var bucketCapacities = buckets.stream()
                                      .map( Bucket::getBufferCapacity )
                                      .collect( Collectors.toList() );
        assertEquals( bucketCapacities, Arrays.asList( capacities ) );
    }

    private static class TestBucketBootstrapper extends BucketBootstrapper
    {
        TestBucketBootstrapper( NeoBufferPoolConfigOverride config )
        {
            super( config, EmptyMemoryTracker.INSTANCE );
        }

        @Override
        protected int getAvailableCpuCount()
        {
            return 16;
        }
    }
}
