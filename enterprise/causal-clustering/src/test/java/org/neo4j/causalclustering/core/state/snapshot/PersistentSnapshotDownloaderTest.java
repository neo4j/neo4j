/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.OPERATION_NAME;

public class PersistentSnapshotDownloaderTest
{
    private final AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "localhost", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress(
            fromAddress );
    private final DatabaseHealth dbHealth = mock( DatabaseHealth.class );

    @Test
    public void shouldPauseAndResumeApplicationProcessIfDownloadIsSuccessful() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        when( coreStateDownloader.downloadSnapshot( any() ) ).thenReturn( true );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        final Log log = mock( Log.class );
        PersistentSnapshotDownloader persistentSnapshotDownloader = new PersistentSnapshotDownloader(
                catchupAddressProvider, applicationProcess, coreStateDownloader, log, new NoTimeout(), () -> dbHealth );

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
        when( coreStateDownloader.downloadSnapshot( any() ) ).thenReturn( false );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        final Log log = mock( Log.class );
        NoTimeout timeout = new NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader = new PersistentSnapshotDownloader(
                catchupAddressProvider, applicationProcess, coreStateDownloader, log, timeout, () -> dbHealth );

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
        when( coreStateDownloader.downloadSnapshot( any() ) ).thenReturn( false );

        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        final Log log = mock( Log.class );
        NoTimeout timeout = new NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader = new PersistentSnapshotDownloader( null,
                applicationProcess, coreStateDownloader, log, timeout, () -> dbHealth );

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
    public void shouldEventuallySucceed()
    {
        // given
        CoreStateDownloader coreStateDownloader = new EventuallySuccessfulDownloader( 3 );

        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        final Log log = mock( Log.class );
        NoTimeout timeout = new NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader = new PersistentSnapshotDownloader(
                catchupAddressProvider, applicationProcess, coreStateDownloader, log, timeout, () -> dbHealth );

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
        when( coreStateDownloader.downloadSnapshot( any() ) ).thenReturn( true );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        final Log log = mock( Log.class );
        PersistentSnapshotDownloader persistentSnapshotDownloader = new PersistentSnapshotDownloader(
                catchupAddressProvider, applicationProcess, coreStateDownloader, log, new NoTimeout(), () -> dbHealth );

        // when
        persistentSnapshotDownloader.run();
        persistentSnapshotDownloader.run();

        // then
        verify( coreStateDownloader, times( 1 ) ).downloadSnapshot( catchupAddressProvider );
        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
    }

    @Test
    public void shouldNotStartIfCurrentlyRunning() throws Exception
    {
        // given
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
        when( coreStateDownloader.downloadSnapshot( any() ) ).thenReturn( false );

        final Log log = mock( Log.class );
        NoTimeout timeout = new NoTimeout();
        PersistentSnapshotDownloader persistentSnapshotDownloader = new PersistentSnapshotDownloader(
                catchupAddressProvider, applicationProcess, coreStateDownloader, log, timeout, () -> dbHealth );

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
            super( null, null, null, null, NullLogProvider.getInstance(), null, null, null, null );
            this.after = after;
        }

        @Override
        boolean downloadSnapshot( CatchupAddressProvider addressProvider )
        {
            return after-- <= 0;
        }
    }
}
