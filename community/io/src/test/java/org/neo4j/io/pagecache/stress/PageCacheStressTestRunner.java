/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.io.pagecache.stress;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.io.pagecache.stress.Conditions.timePeriod;

import org.neo4j.io.pagecache.PageCacheMonitor;

public class PageCacheStressTestRunner
{
    private static final int MillisecondsPerMinute = 60 * 1000;

    public void run( String title, String[] args, SimplePageCacheFactory pageCacheFactory ) throws Exception
    {
        System.out.println( title );
        System.out.println();

        PageCacheStressTest.Builder builder = captureParameters( args );
        System.out.println( builder );

        PageCacheMonitor monitor = new SimpleMonitor();
        PageCacheStressTest runner = builder.with( monitor ).build( pageCacheFactory );

        long startTimeInMilliseconds = currentTimeMillis();
        runner.run();
        long elapsedTimeInMilliseconds = currentTimeMillis() - startTimeInMilliseconds;

        System.out.println( "Test finished in " + elapsedTimeInMilliseconds / MillisecondsPerMinute + " mins." );
        System.out.println( monitor );
    }

    private static PageCacheStressTest.Builder captureParameters( String[] args )
    {
        try
        {
            if ( args.length != 1 )
            {
                throw new IllegalArgumentException( "missing parameters" );
            }

            int duration = Integer.parseInt( args[0] );

            return new PageCacheStressTest.Builder().with( timePeriod( duration, MINUTES ) );
        }
        catch ( NumberFormatException e )
        {
            usage( e );
        }

        throw new IllegalStateException();
    }

    private static void usage( Exception e )
    {
        System.out.println( "error: " + e.getMessage() );
        System.out.println();
        System.out.println( "usage: stresstestrunner <duration in minutes>" );
        System.out.println();
        System.exit( 1 );
    }
}
