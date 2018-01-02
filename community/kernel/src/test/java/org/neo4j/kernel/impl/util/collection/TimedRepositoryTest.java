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
package org.neo4j.kernel.impl.util.collection;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.Consumer;
import org.neo4j.function.Factory;
import org.neo4j.test.ArtificialClock;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimedRepositoryTest
{
    private final AtomicLong valueGenerator = new AtomicLong();
    private final List<Long> reapedValues = new ArrayList<>();

    private final Factory<Long> provider = new Factory<Long>()
    {
        @Override
        public Long newInstance()
        {
            return valueGenerator.getAndIncrement();
        }
    };
    private final Consumer<Long> consumer = new Consumer<Long>()
    {
        @Override
        public void accept( Long value )
        {
            reapedValues.add( value );
        }
    };

    private final long timeout = 100;
    private final ArtificialClock clock = new ArtificialClock();
    private final TimedRepository<Long,Long> repo = new TimedRepository<>( provider, consumer, timeout, clock );

    @Test
    public void shouldManageLifecycleWithNoTimeouts() throws Exception
    {
        // When
        repo.begin( 1l );
        long acquired = repo.acquire( 1l );
        repo.release( 1l );
        repo.end( 1l );

        // Then
        assertThat( acquired, equalTo( 0l ) );
        assertThat( reapedValues, equalTo( asList( 0l ) ) );
    }

    @Test
    public void shouldNotAllowOthersAccessWhenAcquired() throws Exception
    {
        // Given
        repo.begin( 1l );
        repo.acquire( 1l );

        // When
        try
        {
            repo.acquire( 1l );
            fail( "Should not have been allowed access." );
        }
        catch ( ConcurrentAccessException e )
        {
            // Then
            assertThat( e.getMessage(), equalTo( "Cannot access '1', because another client is currently using it." ) );
        }

        // But when
        repo.release( 1l );

        // Then
        assertThat( repo.acquire( 1l ), equalTo( 0l ) );
    }

    @Test
    public void shouldNotAllowAccessAfterEnd() throws Exception
    {
        // Given
        repo.begin( 1l );
        repo.end( 1l );

        // When
        try
        {
            repo.acquire( 1l );
            fail( "Should not have been able to acquire." );
        }
        catch ( NoSuchEntryException e )
        {
            assertThat( e.getMessage(), equalTo( "Cannot access '1', no such entry exists." ) );
        }
    }

    @Test
    public void shouldSilentlyAllowMultipleEndings() throws Exception
    {
        // Given
        repo.begin( 1l );
        repo.end( 1l );

        // When
        repo.end( 1l );

        // Then no exception should've been thrown
    }

    @Test
    public void shouldNotEndImmediatelyIfEntryIsUsed() throws Exception
    {
        // Given
        repo.begin( 1l );
        repo.acquire( 1l );

        // When
        repo.end( 1l );

        // Then
        assertTrue( reapedValues.isEmpty() );

        // But when
        repo.release( 1l );

        // Then
        assertThat( reapedValues, equalTo( asList( 0l ) ) );
    }

    @Test
    public void shouldNotAllowBeginningWithDuplicateKey() throws Exception
    {
        // Given
        repo.begin( 1l );

        // When
        try
        {
            repo.begin( 1l );
            fail( "Should not have been able to begin." );
        }
        catch ( ConcurrentAccessException e )
        {
            assertThat( e.getMessage(), containsString( "Cannot begin '1', because Entry" ) );
            assertThat( e.getMessage(), containsString( " with that key already exists." ) );
        }
    }

    @Test
    public void shouldTimeOutUnusedEntries() throws Exception
    {
        // Given
        repo.begin( 1l );
        repo.acquire( 1l );
        repo.release( 1l );

        // When
        repo.run();

        // Then nothing should've happened, because the entry should not yet get timed out
        assertThat( repo.acquire( 1l ), equalTo( 0l ) );
        repo.release( 1l );

        // But When
        clock.progress( timeout + 1, MILLISECONDS );
        repo.run();

        // Then
        assertThat( reapedValues, equalTo( asList( 0l ) ) );

        try
        {
            repo.acquire( 1l );
            fail( "Should not have been possible to acquire." );
        }
        catch ( NoSuchEntryException e )
        {
            assertThat( e.getMessage(), equalTo( "Cannot access '1', no such entry exists." ) );
        }
    }

    @Test
    public void usingDuplicateKeysShouldDisposeOfPreemptiveAllocatedValue() throws Exception
    {
        // Given
        repo.begin( 1l );

        // When
        try
        {
            repo.begin( 1l );
            fail( "Should not have been able to begin." );
        }
        catch ( ConcurrentAccessException e )
        {

            // Then
            assertThat( e.getMessage(), containsString( "Cannot begin '1', because Entry" ) );
            assertThat( e.getMessage(), containsString( " with that key already exists." ) );
        }
        assertThat( reapedValues, equalTo( asList( 1l ) ) );
    }

    @Test
    public void shouldAllowBeginWithSameKeyAfterSessionRelease() throws Exception
    {
        // Given
        repo.begin( 1l );
        repo.acquire( 1l );

        // when
        repo.release( 1l );
        repo.end( 1l );

        //then
        repo.begin( 1l );
        assertThat( reapedValues, equalTo( asList( 0l ) ) );
    }

    @Test
    public void unusedEntriesSafelyAcquiredOnCleanup()
            throws ConcurrentAccessException, NoSuchEntryException, InterruptedException, TimeoutException,
            BrokenBarrierException
    {
        CountDownReaper countDownReaper = new CountDownReaper();
        final TimedRepository<Object,Long> timedRepository = new TimedRepository<>( provider, countDownReaper, 1, clock );
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        NonStoppableCleaner cleaner = new NonStoppableCleaner( timedRepository );
        try
        {
            singleThreadExecutor.submit( cleaner );

            long entryKey = 1L;
            long iterations = 100000L;
            while ( entryKey++ < iterations )
            {
                timedRepository.begin( entryKey );
                timedRepository.acquire( entryKey );
                clock.progress( 10, TimeUnit.MILLISECONDS );
                timedRepository.release( entryKey );
                timedRepository.end( entryKey );

                countDownReaper.await("Reaper should consume entry from cleaner thread or from our 'end' call. " +
                                      "If it was not consumed it mean cleaner and worker thread where not able to" +
                                      " figure out who removes entry, and block will ends up in the repo forever.",
                                      10, SECONDS);
                countDownReaper.reset();
            }
        }
        finally
        {
            cleaner.stop();
            singleThreadExecutor.shutdownNow();
        }
    }

    private static class NonStoppableCleaner implements Runnable
    {
        private volatile boolean stop = false;
        private final TimedRepository<Object,Long> timedRepository;

        NonStoppableCleaner( TimedRepository<Object,Long> timedRepository )
        {
            this.timedRepository = timedRepository;
        }

        @Override
        public void run()
        {
            while (!stop)
            {
                timedRepository.run();
            }
        }

        public void stop()
        {
            stop = true;
        }
    }

    private static class CountDownReaper implements Consumer<Long>
    {
        private volatile CountDownLatch reaperLatch;

        CountDownReaper( )
        {
            reset();
        }

        public void reset()
        {
            reaperLatch = new CountDownLatch( 1 );
        }

        @Override
        public void accept( Long aLong )
        {
            reaperLatch.countDown();
        }

        public void await(String message, long timeout, TimeUnit timeUnit) throws InterruptedException
        {
            if ( !reaperLatch.await( timeout, timeUnit ) )
            {
                throw new IllegalStateException( message );
            }
        }

    }
}
