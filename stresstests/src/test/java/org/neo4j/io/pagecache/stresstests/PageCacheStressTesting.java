/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.pagecache.stresstests;

import org.junit.Test;

import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.stress.PageCacheStressTest;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.io.pagecache.stress.Conditions.timePeriod;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
public class PageCacheStressTesting
{
    static {
        // Pin/Unpin monitoring is disabled by default for performance reasons,
        // but we have tests that verify that pinned and unpinned are called
        // correctly.
        DefaultPageCacheTracer.enablePinUnpinTracing();
    }

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        int durationInMinutes = parseInt( fromEnvironmentOrDefault( "PAGE_CACHE_STRESS_DURATION", "1" ) );
        int numberOfPages = parseInt( fromEnvironmentOrDefault( "PAGE_CACHE_STRESS_NUMBER_OF_PAGES", "10000" ) );
        int recordsPerPage = parseInt( fromEnvironmentOrDefault( "PAGE_CACHE_STRESS_RECORDS_PER_PAGE", "113" ) );
        int numberOfThreads = parseInt( fromEnvironmentOrDefault( "PAGE_CACHE_STRESS_NUMBER_OF_THREADS", "8" ) );
        int cachePagePadding = parseInt( fromEnvironmentOrDefault( "PAGE_CACHE_STRESS_CACHE_PAGE_PADDING", "56" ) );
        int numberOfCachePages = parseInt( fromEnvironmentOrDefault( "PAGE_CACHE_STRESS_NUMBER_OF_CACHE_PAGES", "1000" ) );

        String workingDirectory = fromEnvironmentOrDefault( "PAGE_CACHE_STRESS_WORKING_DIRECTORY", getProperty( "java.io.tmpdir" ) );

        DefaultPageCacheTracer monitor = new DefaultPageCacheTracer();

        PageCacheStressTest runner = new PageCacheStressTest.Builder()
                .with( timePeriod( durationInMinutes, MINUTES ) )
                .withNumberOfPages( numberOfPages )
                .withRecordsPerPage( recordsPerPage )
                .withNumberOfThreads( numberOfThreads )
                .withCachePagePadding( cachePagePadding )
                .withNumberOfCachePages( numberOfCachePages )
                .withWorkingDirectory(workingDirectory)
                .with( monitor )
                .build();

        runner.run();

        long faults = monitor.countFaults();
        long evictions = monitor.countEvictions();
        long pins = monitor.countPins();
        long unpins = monitor.countUnpins();
        long flushes = monitor.countFlushes();
        System.out.printf( " - page faults: %d%n - evictions: %d%n - pins: %d%n - unpins: %d%n - flushes: %d%n",
                faults, evictions, pins, unpins, flushes );
    }

    private static String fromEnvironmentOrDefault( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );

        if ( environmentVariableValue == null )
        {
            return defaultValue;
        }

        return environmentVariableValue;
    }
}
