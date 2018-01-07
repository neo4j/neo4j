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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FSALockFactoryTest
{
    private static final int N_THREADS = 7;
    private static final int SECONDS_TO_RUN = 5;

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static int seed = new Random().nextInt();
    private static Random randomSeeds = new Random( seed );

    private static Map<String,Thread> heldLocks = new ConcurrentHashMap<>();
    private static AtomicLong locksObtained = new AtomicLong();

    @Test
    public void shouldNotAllowTwoActorsToHoldSameLock() throws Exception
    {
        // Given
        long timeout = System.currentTimeMillis() + 1000 * SECONDS_TO_RUN;
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        Path path = tmpFolder.newFolder().toPath();
        fs.mkdirs( path.toFile() );

        ExecutorService executor = Executors.newFixedThreadPool( N_THREADS );
        Stream<Future<String>> results = range( 0, N_THREADS ).mapToObj(
                n -> executor.submit(
                        acquireAndAssertWorker( timeout, pageCache, path ) ) );

        // Then
        results.forEach( assertAllLocksAcquiredCorrectly() );
        assertThat( locksObtained.get(), greaterThan( (long) N_THREADS ) );
    }

    /**
     * A little worker that acquires a random lock, notes that it thinks it
     * holds it and asserts
     * nobody else thinks the same. If the assertion fails, it returns a string
     * description of the
     * error.
     *
     * @param timeout run until this unix timestamp
     * @param pageCache
     * @param path
     * @return
     */
    private Callable<String> acquireAndAssertWorker( long timeout,
            PageCache pageCache, Path path )
    {
        return () ->
        {
            FSALockFactory locks = new FSALockFactory( fs );
            Directory dir = new PagedDirectory( path, pageCache );
            Random rand = new Random( randomSeeds.nextInt() );
            Thread thisThread = Thread.currentThread();

            do
            {
                try
                {
                    String lockName = "lock-" + rand.nextInt( 100 );
                    Lock lock = locks.obtainLock( dir, lockName );
                    try
                    {
                        Thread otherThread =
                                heldLocks.putIfAbsent( lockName, thisThread );
                        if ( otherThread != null )
                        {
                            return "Assertion error: " + thisThread.getName() +
                                    " was able to grab a lock that " +
                                    otherThread.getName() + " already held: " +
                                    lockName;
                        }
                        locksObtained.incrementAndGet();
                        Thread.sleep( 1 );
                        lock.close();
                    }
                    finally
                    {
                        heldLocks.remove( lockName, thisThread );
                    }
                }
                catch ( LockObtainFailedException e )
                {
                    // Fine
                }
            }
            while ( System.currentTimeMillis() < timeout );

            return null;
        };
    }

    private Consumer<Future<String>> assertAllLocksAcquiredCorrectly()
    {
        return result ->
        {
            try
            {
                String assertionError =
                        result.get( SECONDS_TO_RUN * 100, TimeUnit.SECONDS );
                if ( assertionError != null )
                {
                    fail( assertionError );
                }
            }
            catch ( InterruptedException | ExecutionException | TimeoutException e )
            {
                throw Exceptions.launderedException( e );
            }
        };
    }
}
