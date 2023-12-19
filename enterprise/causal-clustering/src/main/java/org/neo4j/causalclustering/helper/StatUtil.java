/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.helper;

import org.neo4j.logging.Log;

import static java.lang.String.format;
import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

@SuppressWarnings( "unused" ) // for easy debugging, leave it
public class StatUtil
{
    public static class StatContext
    {
        private static final int N_BUCKETS = 10; // values >= Math.pow( 10, N_BUCKETS-1 ) all go into the last bucket

        private final String name;
        private final Log log;
        private final long printEvery;
        private final boolean clearAfterPrint;
        private BasicStats[] bucket = new BasicStats[N_BUCKETS];
        private long totalCount;

        private StatContext( String name, Log log, long printEvery, boolean clearAfterPrint )
        {
            this.name = name;
            this.log = log;
            this.printEvery = printEvery;
            this.clearAfterPrint = clearAfterPrint;
            clear();
        }

        public synchronized void clear()
        {
            for ( int i = 0; i < N_BUCKETS; i++ )
            {
                bucket[i] = new BasicStats();
            }
            totalCount = 0;
        }

        public void collect( double value )
        {
            int bucketIndex = bucketFor( value );

            synchronized ( this )
            {
                totalCount++;
                bucket[bucketIndex].collect( value );

                if ( totalCount % printEvery == 0 )
                {
                    print();
                }
            }
        }

        private int bucketFor( double value )
        {
            int bucketIndex;
            if ( value <= 0 )
            {
                // we do not have buckets for negative values, we assume user doesn't measure such things
                // however, if they do, it will all be collected in bucket 0
                bucketIndex = 0;
            }
            else
            {
                bucketIndex = (int) Math.log10( value );
                bucketIndex = Math.min( bucketIndex, N_BUCKETS - 1 );
            }
            return bucketIndex;
        }

        public TimingContext time()
        {
            return new TimingContext( this );
        }

        public synchronized void print()
        {
            for ( BasicStats stats : bucket )
            {
                if ( stats.count > 0 )
                {
                    log.info( format( "%s%s", name, stats ) );
                }
            }

            if ( clearAfterPrint )
            {
                clear();
            }
        }
    }

    private StatUtil()
    {
    }

    public static synchronized StatContext create( String name, long printEvery, boolean clearAfterPrint )
    {
        return create( name, toOutputStream( System.out ).getLog( name ), printEvery, clearAfterPrint );
    }

    public static synchronized StatContext create( String name, Log log, long printEvery, boolean clearAfterPrint )
    {
        return new StatContext( name, log, printEvery, clearAfterPrint );
    }

    public static class TimingContext
    {
        private final StatContext context;
        private final long startTime = System.currentTimeMillis();

        TimingContext( StatContext context )
        {
            this.context = context;
        }

        public void end()
        {
            context.collect( System.currentTimeMillis() - startTime );
        }
    }

    private static class BasicStats
    {
        private Double min;
        private Double max;

        private Double avg = 0.;
        private long count;

        void collect( double val )
        {
            count++;
            avg = avg + (val - avg) / count;

            min = min == null ? val : Math.min( min, val );
            max = max == null ? val : Math.max( max, val );
        }

        @Override
        public String toString()
        {
            return format( "{min=%s, max=%s, avg=%s, count=%d}", min, max, avg, count );
        }
    }
}
