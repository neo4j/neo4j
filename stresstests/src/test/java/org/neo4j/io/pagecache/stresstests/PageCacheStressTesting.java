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

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.stress.PageCacheStressTest;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.helper.StressTestingHelper.fromEnv;
import static org.neo4j.io.pagecache.stress.Conditions.timePeriod;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
public class PageCacheStressTesting
{

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        int durationInMinutes = parseInt( fromEnv( "PAGE_CACHE_STRESS_DURATION", "1" ) );
        int numberOfPages = parseInt( fromEnv( "PAGE_CACHE_STRESS_NUMBER_OF_PAGES", "10000" ) );
        int numberOfThreads = parseInt( fromEnv( "PAGE_CACHE_STRESS_NUMBER_OF_THREADS", "8" ) );
        int numberOfCachePages = parseInt( fromEnv( "PAGE_CACHE_STRESS_NUMBER_OF_CACHE_PAGES", "1000" ) );
        File baseDir = new File( fromEnv( "PAGE_CACHE_STRESS_WORKING_DIRECTORY", getProperty( "java.io.tmpdir" ) ) );

        File workingDirectory = new File( baseDir,  "working" );

        DefaultPageCacheTracer monitor = new DefaultPageCacheTracer();
        PageCacheStressTest runner = new PageCacheStressTest.Builder()
                .with( timePeriod( durationInMinutes, MINUTES ) )
                .withNumberOfPages( numberOfPages )
                .withNumberOfThreads( numberOfThreads )
                .withNumberOfCachePages( numberOfCachePages )
                .withWorkingDirectory( ensureExistsAndEmpty( workingDirectory ) )
                .with( monitor )
                .build();

        runner.run();

        long faults = monitor.faults();
        long evictions = monitor.evictions();
        long pins = monitor.pins();
        long unpins = monitor.unpins();
        long flushes = monitor.flushes();
        System.out.printf( " - page faults: %d%n - evictions: %d%n - pins: %d%n - unpins: %d%n - flushes: %d%n",
                faults, evictions, pins, unpins, flushes );

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( workingDirectory );
    }
}
