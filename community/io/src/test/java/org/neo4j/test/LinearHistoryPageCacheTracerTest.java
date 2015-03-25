/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

public class LinearHistoryPageCacheTracerTest
{
    @Ignore( "This test is only here for checking that the output from the LinearHistoryPageCacheTracer looks good. " +
             "This is pretty subjective and requires manual inspection. Therefore there's no point in running it " +
             "automatically in all our builds. Instead, run it as needed when you make changes to the printout code." )
    @Test
    public void makeSomeTestOutput() throws Exception
    {
        RandomAdversary adversary = new RandomAdversary( 0.1, 0.1, 0.0 );
        adversary.setProbabilityFactor( 0.0 );
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        AdversarialFileSystemAbstraction afs = new AdversarialFileSystemAbstraction( adversary, fs );
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory( afs );
        int maxPages = 10;
        int cachePageSize = 8192;
        LinearHistoryPageCacheTracer tracer = new LinearHistoryPageCacheTracer();

        File fileA = new File( "a" );
        File fileB = new File( "b" );
        fs.open( fileA, "rw" ).close();
        fs.open( fileB, "rw" ).close();

        try ( MuninnPageCache cache = new MuninnPageCache( swapperFactory, maxPages, cachePageSize, tracer );
              PagedFile pfA = cache.map( fileA, 8192 );
              PagedFile pfB = cache.map( fileB, 8192 ) )
        {
            adversary.setProbabilityFactor( 1.0 );
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            for ( int i = 0; i < 200; i++ )
            {
                try
                {
                    boolean readOnly = rng.nextBoolean();
                    int flags = readOnly ? PF_SHARED_LOCK : PF_EXCLUSIVE_LOCK;
                    int startPage = rng.nextInt( 0, 10 );
                    int iterations = rng.nextInt( 1, 10 );
                    PagedFile file = rng.nextBoolean()? pfA : pfB;
                    try ( PageCursor cursor = file.io( startPage, flags ) )
                    {
                        for ( int j = 0; j < iterations; j++ )
                        {
                            cursor.next();
                            Thread.sleep( 1 );
                            if ( !readOnly )
                            {
                                for ( int k = 0; k < 8192 / 4; k++ )
                                {
                                    cursor.putInt( rng.nextInt() );
                                }
                            }
                        }
                    }
                    if ( rng.nextDouble() < 0.1 )
                    {
                        file.flushAndForce();
                    }
                    else if ( rng.nextBoolean() )
                    {
                        cache.flushAndForce();
                    }
                }
                catch ( Throwable ignore )
                {
                }
            }

            // Don't fail when we close or unmap.
            adversary.setProbabilityFactor( 0.0 );
        }

        tracer.printHistory( System.out );
    }
}
