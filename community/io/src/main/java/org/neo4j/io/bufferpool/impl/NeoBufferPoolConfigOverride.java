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

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.io.ByteUnit;

public class NeoBufferPoolConfigOverride
{
    private final Duration collectionInterval;
    private final Integer tinyBufferThreshold;
    private final List<Bucket> buckets;

    public NeoBufferPoolConfigOverride( Duration collectionInterval, Integer tinyBufferThreshold, List<String> buckets )
    {
        this.collectionInterval = collectionInterval;
        this.tinyBufferThreshold = tinyBufferThreshold;
        this.buckets = buckets.stream()
                              .map( NeoBufferPoolConfigOverride::parseBucketExpression )
                              .collect( Collectors.toList() );
    }

    private static Bucket parseBucketExpression( String bucketExpression )
    {
        // The format is <buffer size>:<slice expression>,
        // where <buffer size> has format supported by ByteUnit.parse
        // and <slice expression> is either an integer representing
        // an absolute number of slices or a double suffixed by letter 'C'
        // which represents a multiplier of number of cores.
        // For instance "4K:8" means a bucket for 4k buffers with 8 slices
        // and "8k:1.5C" means a bucket for 8k buffers with slices equal to
        // 1.5 times the number of available processors.

        String[] parts = bucketExpression.split( ":" );
        if ( parts.length != 2 )
        {
            throw incorrectFormatException( bucketExpression );
        }

        int bufferSize = (int) ByteUnit.parse( parts[0] );
        String sliceExpression = parts[1];
        if ( sliceExpression.charAt( sliceExpression.length() - 1 ) == 'C' )
        {
            String coefficientStr = sliceExpression.substring( 0, sliceExpression.length() - 1 );
            try
            {
                double coefficient = Double.parseDouble( coefficientStr );
                return new Bucket( bufferSize, coefficient );
            }
            catch ( Exception e )
            {
                throw incorrectFormatException( bucketExpression );
            }
        }
        else
        {
            try
            {
                int sliceCount = Integer.parseInt( sliceExpression );
                return new Bucket( bufferSize, sliceCount );
            }
            catch ( Exception e )
            {
                throw incorrectFormatException( bucketExpression );
            }
        }
    }

    private static RuntimeException incorrectFormatException( String bucketExpression )
    {
        return new IllegalArgumentException( "Incorrect format of bucket expression: " + bucketExpression );
    }

    public Duration getCollectionInterval()
    {
        return collectionInterval;
    }

    public Integer getTinyBufferThreshold()
    {
        return tinyBufferThreshold;
    }

    public List<Bucket> getBuckets()
    {
        return buckets;
    }

    static class Bucket
    {
        private final int bufferCapacity;
        private final Integer sliceCount;
        private final Double sliceCoefficient;

        Bucket( int bufferCapacity, int sliceCount )
        {
            this.bufferCapacity = bufferCapacity;
            this.sliceCoefficient = null;
            this.sliceCount = sliceCount;
        }

        Bucket( int bufferCapacity, double sliceCoefficient )
        {
            this.bufferCapacity = bufferCapacity;
            this.sliceCount = null;
            this.sliceCoefficient = sliceCoefficient;
        }

        int getBufferCapacity()
        {
            return bufferCapacity;
        }

        Integer getSliceCount()
        {
            return sliceCount;
        }

        Double getSliceCoefficient()
        {
            return sliceCoefficient;
        }
    }
}
