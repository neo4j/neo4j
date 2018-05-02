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
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.util.CountingJobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.OPERATION_NAME;

public class CoreStateDownloaderServiceTest
{
    private final AdvertisedSocketAddress someMemberAddress = new AdvertisedSocketAddress( "localhost", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( someMemberAddress );
    private CentralJobScheduler centralJobScheduler;
    private DatabaseHealth dbHealth = mock( DatabaseHealth.class );

    @Before
    public void create()
    {
        centralJobScheduler = new CentralJobScheduler();
        centralJobScheduler.init();
    }

    @After
    public void shutdown()
    {
        centralJobScheduler.shutdown();
    }

    @Test
    public void shouldRunPersistentDownloader() throws Exception
    {
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        when( coreStateDownloader.downloadSnapshot( any() ) ).thenReturn( true );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        final Log log = mock( Log.class );
        CoreStateDownloaderService coreStateDownloaderService =
                new CoreStateDownloaderService( centralJobScheduler, coreStateDownloader, applicationProcess,
                        logProvider( log ), new NoTimeout(), () -> dbHealth, new Monitors() );
        coreStateDownloaderService.scheduleDownload( catchupAddressProvider );
        waitForApplierToResume( applicationProcess );

        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        verify( coreStateDownloader, times( 1 ) ).downloadSnapshot( any() );
    }

    @Test
    public void shouldOnlyScheduleOnePersistentDownloaderTaskAtTheTime() throws InterruptedException
    {
        AtomicInteger schedules = new AtomicInteger();
        CountingJobScheduler countingJobScheduler = new CountingJobScheduler( schedules, centralJobScheduler );
        Semaphore blockDownloader = new Semaphore( 0 );
        CoreStateDownloader coreStateDownloader = new BlockingCoreStateDownloader( blockDownloader );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        final Log log = mock( Log.class );
        CoreStateDownloaderService coreStateDownloaderService =
                new CoreStateDownloaderService( countingJobScheduler, coreStateDownloader, applicationProcess,
                        logProvider( log ), new NoTimeout(), () -> dbHealth, new Monitors() );

        coreStateDownloaderService.scheduleDownload( catchupAddressProvider );
        Thread.sleep( 50 );
        coreStateDownloaderService.scheduleDownload( catchupAddressProvider );
        coreStateDownloaderService.scheduleDownload( catchupAddressProvider );
        coreStateDownloaderService.scheduleDownload( catchupAddressProvider );

        assertEquals( 1, schedules.get() );
        blockDownloader.release();
    }

    static class BlockingCoreStateDownloader extends CoreStateDownloader
    {
        private final Semaphore semaphore;

        BlockingCoreStateDownloader( Semaphore semaphore )
        {
            super( null, null, null, null, NullLogProvider.getInstance(), null,
                    null, null, null );
            this.semaphore = semaphore;
        }

        @Override
        boolean downloadSnapshot( CatchupAddressProvider addressProvider )
        {
            semaphore.acquireUninterruptibly();
            return true;
        }
    }

    private void waitForApplierToResume( CommandApplicationProcess applicationProcess ) throws TimeoutException
    {
        Predicates.await( () ->
        {
            try
            {
                verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
                return true;
            }
            catch ( Throwable t )
            {
                return false;
            }
        }, 20, TimeUnit.SECONDS );
    }

    private LogProvider logProvider( Log log )
    {
        return new LogProvider()
        {
            @Override
            public Log getLog( Class loggingClass )
            {
                return log;
            }

            @Override
            public Log getLog( String name )
            {
                return log;
            }
        };
    }
}
