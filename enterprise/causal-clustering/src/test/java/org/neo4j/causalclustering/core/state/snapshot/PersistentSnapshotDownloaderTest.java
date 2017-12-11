/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.function.Predicates;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.OPERATION_NAME;

public class PersistentSnapshotDownloaderTest
{
    private final MemberId someMember = new MemberId( UUID.randomUUID() );

    @Test
    public void shouldPauseAndResumeApplicationProcessIfDownloadIsSuccessful() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        final Log log = mock( Log.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( someMember );
        PersistentSnapshotDownloader persistentSnapshotDownloader =
                new PersistentSnapshotDownloader( leaderLocator, applicationProcess, coreStateDownloader, log,
                        new NoTimeout() );

        // when
        persistentSnapshotDownloader.run();

        // then
        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        verify( coreStateDownloader, times( 1 ) ).downloadSnapshot( any() );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldResumeCommandApplicationProcessIfInterrupted() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        doThrow( StoreCopyFailedException.class ).when( coreStateDownloader ).downloadSnapshot( someMember );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( someMember );

        final Log log = mock( Log.class );
        NoTimeout timeout = new NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader =
                new PersistentSnapshotDownloader( leaderLocator, applicationProcess, coreStateDownloader, log,
                        timeout );

        // when
        Thread thread = new Thread( persistentSnapshotDownloader );
        thread.start();
        awaitOneIteration( timeout );
        thread.interrupt();
        thread.join();

        // then
        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldResumeCommandApplicationProcessIfDownloaderIsStopped() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        doThrow( NoLeaderFoundException.class ).when( leaderLocator ).getLeader();

        final Log log = mock( Log.class );
        NoTimeout timeout = new
                NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader =
                new PersistentSnapshotDownloader( leaderLocator, applicationProcess, coreStateDownloader, log,
                        timeout );

        // when
        Thread thread = new Thread( persistentSnapshotDownloader );
        thread.start();
        awaitOneIteration( timeout );
        persistentSnapshotDownloader.stop();
        thread.join();

        // then
        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldEventuallySucceed() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = new EventuallySuccessfulDownloader( 3 );

        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( someMember );
        final Log log = mock( Log.class );
        NoTimeout timeout = new NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader =
                new PersistentSnapshotDownloader( leaderLocator, applicationProcess, coreStateDownloader, log,
                        timeout );

        // when
        persistentSnapshotDownloader.run();

        // then
        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        assertEquals( 3, timeout.currentCount() );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldNotStartDownloadIfAlreadyCompleted() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( someMember );

        final Log log = mock( Log.class );
        PersistentSnapshotDownloader persistentSnapshotDownloader =
                new PersistentSnapshotDownloader( leaderLocator, applicationProcess, coreStateDownloader, log,
                        new NoTimeout() );

        // when
        persistentSnapshotDownloader.run();
        persistentSnapshotDownloader.run();

        // then
        verify( coreStateDownloader, times( 1 ) ).downloadSnapshot( someMember );
        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
    }

    @Test
    public void shouldNotStartIfCurrentlyRunning() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        doThrow( NoLeaderFoundException.class ).when( leaderLocator ).getLeader();

        final Log log = mock( Log.class );
        NoTimeout timeout = new NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader =
                new PersistentSnapshotDownloader( leaderLocator, applicationProcess, coreStateDownloader, log,
                        timeout );

        Thread thread = new Thread( persistentSnapshotDownloader );

        // when
        thread.start();
        awaitOneIteration( timeout );
        persistentSnapshotDownloader.run();
        persistentSnapshotDownloader.stop();
        thread.join();

        // then
        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
    }

    private void awaitOneIteration( NoTimeout timeout ) throws TimeoutException
    {
        Predicates.await( () -> timeout.currentCount() > 0, 1, TimeUnit.SECONDS );
    }

    private class EventuallySuccessfulDownloader extends CoreStateDownloader
    {
        private int after;

        private EventuallySuccessfulDownloader( int after )
        {
            super( null, null, null,
                    null, NullLogProvider.getInstance(), null, null,
                    null, null );
            this.after = after;
        }

        @Override
        void downloadSnapshot( MemberId source ) throws StoreCopyFailedException
        {
            if ( after-- > 0 )
            {
                throw new StoreCopyFailedException( "sorry" );
            }
        }
    }

}
