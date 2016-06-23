/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.helper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings( "unused" ) // for easy debugging, leave it
public class StatUtil
{
    private static class StatPrinterService extends ScheduledThreadPoolExecutor
    {
        private static final StatPrinterService INSTANCE = new StatPrinterService();

        private StatPrinterService()
        {
            super( 1, new NamedThreadFactory( "stat-printer" , Thread.NORM_PRIORITY, true ) );
            super.setRemoveOnCancelPolicy( true );
        }
    }

    public static class StatContext
    {
        private static final int N_BUCKETS = 10; // values >= Math.pow( 10, N_BUCKETS-1 ) all go into the last bucket

        private String name;
        private BasicStats[] bucket = new BasicStats[N_BUCKETS];
        private long totalCount;

        private StatContext( String name, Log log )
        {
            this.name = name;
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

        public long totalCount()
        {
            return totalCount;
        }

        public synchronized String getText( boolean clear )
        {
            String text = toString();

            if( clear )
            {
                clear();
            }

            return text;
        }

        @Override
        public synchronized String toString()
        {
            StringBuilder output = new StringBuilder();
            for ( BasicStats stats : bucket )
            {
                if( stats.count > 0 )
                {
                    output.append( format( "%n%s", stats ) );
                }
            }
            return output.toString();
        }
    }

    public static StatContext create( String name )
    {
        return new StatContext( name, NullLogProvider.getInstance().getLog( name ) );
    }

    private static Map<String,ScheduledFuture> printingJobs = new HashMap<>();

    public static synchronized StatContext create( String name, long printEveryMs )
    {
        return create( name, FormattedLogProvider.toOutputStream( System.out ).getLog( name ), printEveryMs, false, false );
    }

    public static synchronized StatContext create( String name, Log log, long printEveryMs, boolean clearAfterPrint, boolean ensureUnique )
    {
        StatContext statContext = new StatContext( name, log );
        ScheduledFuture job = null;

        if ( ensureUnique )
        {
            job = printingJobs.remove( name );
        }

        if ( job != null )
        {
            job.cancel( true );
            log.warn( "Replacing printer for: " + name );
        }

        job = StatPrinterService.INSTANCE.scheduleAtFixedRate( () -> {
            if ( statContext.totalCount() > 0 )
            {
                String data = statContext.getText( clearAfterPrint );
                log.info( "%s%s", name, data );
            }
        }, printEveryMs, printEveryMs, MILLISECONDS );

        if ( ensureUnique )
        {
            printingJobs.put( name, job );
        }

        return statContext;
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
