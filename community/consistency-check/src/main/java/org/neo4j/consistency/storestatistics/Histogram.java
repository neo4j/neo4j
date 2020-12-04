/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.storestatistics;

import java.util.Arrays;

/**
 * Basic histogram implementation used to represent statistics about the database store analysed by the consistency checker.
 */
class Histogram
{
    static final int[] COUNT_BUCKETS = {0, 1, 1 << 2, 1 << 4, 1 << 6, 1 << 8, 1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 24, Integer.MAX_VALUE};
    static final int[] FRAG_MEASURE_BUCKETS = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};

    private final int[] buckets;
    private final int[] frequencies;

    /**
     * Returns a histogram ready to be used to hold frequency data about integer values.
     */
    Histogram()
    {
        this( COUNT_BUCKETS );
    }

    /**
     * Returns a histogram ready to be used to hold frequency data about frequency measures.
     */
    static Histogram newFragMeasureHistogram()
    {
        return new Histogram( FRAG_MEASURE_BUCKETS );
    }

    private Histogram( int[] bucketUpperBounds )
    {
        this.buckets = bucketUpperBounds;
        this.frequencies = new int[bucketUpperBounds.length];
    }

    /**
     * Add the frequencies of this histogram to an aggregate data histogram. Used when, for example, multiple threads collect statistics separately and their
     * data needs to be aggregated at the end.
     *
     * @param totalHist the histogram holding aggregate data.
     */
    void addTo( Histogram totalHist )
    {
        if ( !Arrays.equals( this.buckets, totalHist.buckets ) )
        {
            throw new RuntimeException( "Attempted to add histograms with different buckets. This is a bug." );
        }
        for ( int i = 0; i < frequencies.length; i++ )
        {
            totalHist.frequencies[i] += frequencies[i];
        }
    }

    /**
     * Increments the frequency of the bucket corresponding to the given value.
     *
     * @param value the value to the be added to histogram.
     */
    void addValue( int value )
    {
        for ( int i = 0; i < buckets.length; i++ )
        {
            if ( value <= buckets[i] )
            {
                frequencies[i]++;
                return;
            }
        }
        throw new RuntimeException( String.format( "Attempted to add value %d to histogram whose max value is %d. This is a bug.",
                                                   value,
                                                   buckets[buckets.length - 1] ) );
    }

    /**
     * Create a string that makes the histogram data easily digestible by humans.
     *
     * @return textual representation of the histogram.
     */
    String prettyPrint()
    {
        StringBuilder sb = new StringBuilder();

        // Width of the left side of the histogram, so that all numbers fit
        int width = 0;
        int tmp = buckets[buckets.length - 1];
        while ( tmp > 0 )
        {
            width++;
            tmp /= 10;
        }
        width = Math.max( width, "Buckets".length() );

        String bucketsFormat = String.format( "%%%ds", width );

        sb.append( String.format( bucketsFormat, "Buckets" ) );
        sb.append( " | " );
        sb.append( "Frequencies" );

        for ( int i = 0; i < buckets.length; i++ )
        {
            sb.append( "\n" );
            sb.append( String.format( bucketsFormat, buckets[i] ) );
            sb.append( " | " );
            sb.append( frequencies[i] );
        }
        return sb.toString();
    }
}
