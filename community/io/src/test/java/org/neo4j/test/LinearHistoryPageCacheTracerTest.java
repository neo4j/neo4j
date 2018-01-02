/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Consumers;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.randomharness.Phase;
import org.neo4j.io.pagecache.randomharness.RandomPageCacheTestHarness;

public class LinearHistoryPageCacheTracerTest
{
    @Ignore( "This test is only here for checking that the output from the LinearHistoryPageCacheTracer looks good. " +
             "This is pretty subjective and requires manual inspection. Therefore there's no point in running it " +
             "automatically in all our builds. Instead, run it as needed when you make changes to the printout code." )
    @Test
    public void makeSomeTestOutput() throws Exception
    {
        final LinearHistoryPageCacheTracer tracer = new LinearHistoryPageCacheTracer();
        RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness();
        harness.setUseAdversarialIO( true );
        harness.setTracer( tracer );
        harness.setCommandCount( 100 );
        harness.setConcurrencyLevel( 2 );
        harness.setPreparation( new Phase()
        {
            @Override
            public void run( PageCache pageCache, FileSystemAbstraction fs, Set<File> files )
            {
                tracer.processHistory( Consumers.<LinearHistoryPageCacheTracer.HEvent>noop() );
            }
        } );

        harness.run( 1, TimeUnit.MINUTES );

        tracer.printHistory( System.out );
    }
}
