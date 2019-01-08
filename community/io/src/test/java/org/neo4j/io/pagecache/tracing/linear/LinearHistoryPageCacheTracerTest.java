/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.pagecache.tracing.linear;

import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.io.pagecache.randomharness.RandomPageCacheTestHarness;

public class LinearHistoryPageCacheTracerTest
{
    @Ignore( "This test is only here for checking that the output from the LinearHistoryPageCacheTracer looks good. " +
             "This is pretty subjective and requires manual inspection. Therefore there's no point in running it " +
             "automatically in all our builds. Instead, run it as needed when you make changes to the printout code." )
    @Test
    public void makeSomeTestOutput() throws Exception
    {
        LinearTracers linearTracers = LinearHistoryTracerFactory.pageCacheTracer();
        try ( RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness() )
        {
            harness.setUseAdversarialIO( true );
            harness.setTracer( linearTracers.getPageCacheTracer() );
            harness.setCursorTracerSupplier( linearTracers.getCursorTracerSupplier() );
            harness.setCommandCount( 100 );
            harness.setConcurrencyLevel( 2 );
            harness.setPreparation( ( pageCache, fs, files ) -> linearTracers.processHistory( hEvent -> {} ) );

            harness.run( 1, TimeUnit.MINUTES );

            linearTracers.printHistory( System.out );
        }

    }
}
