/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.impl.util.CountingJobScheduler;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.OPERATION_NAME;

public class CoreStateDownloaderServiceTest
{
    private final MemberId someMember = new MemberId( UUID.randomUUID() );
    private Neo4jJobScheduler neo4jJobScheduler;

    @Before
    public void create()
    {
        neo4jJobScheduler = new Neo4jJobScheduler();
        neo4jJobScheduler.init();
    }

    @After
    public void shutdown()
    {
        neo4jJobScheduler.shutdown();
    }

    @Test
    public void shouldRunPersistentDownloader() throws Exception
    {
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        final Log log = mock( Log.class );
        CoreStateDownloaderService coreStateDownloaderService =
                new CoreStateDownloaderService( neo4jJobScheduler, coreStateDownloader, applicationProcess,
                        logProvider( log ), new NoTimeout() );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( someMember );
        coreStateDownloaderService.scheduleDownload( leaderLocator );
        waitForApplierToResume( applicationProcess );

        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        verify( coreStateDownloader, times( 1 ) ).downloadSnapshot( any() );
    }

    @Test
    public void shouldOnlyScheduleOnePersistentDownloaderTaskAtTheTime() throws Exception
    {
        AtomicInteger schedules = new AtomicInteger();
        CountingJobScheduler countingJobScheduler = new CountingJobScheduler( schedules, neo4jJobScheduler );
        CoreStateDownloader coreStateDownloader = mock( CoreStateDownloader.class );
        final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        final Log log = mock( Log.class );
        CoreStateDownloaderService coreStateDownloaderService =
                new CoreStateDownloaderService( countingJobScheduler, coreStateDownloader, applicationProcess,
                        logProvider( log ), new NoTimeout() );

        AtomicBoolean availableLeader = new AtomicBoolean( false );

        LeaderLocator leaderLocator = new ControllableLeaderLocator( availableLeader );
        coreStateDownloaderService.scheduleDownload( leaderLocator );
        coreStateDownloaderService.scheduleDownload( leaderLocator );
        coreStateDownloaderService.scheduleDownload( leaderLocator );
        coreStateDownloaderService.scheduleDownload( leaderLocator );

        availableLeader.set( true );

        assertEquals( 1, schedules.get() );
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
        }, 1, TimeUnit.SECONDS );
    }

    private class ControllableLeaderLocator implements LeaderLocator
    {
        private final AtomicBoolean shouldProvideALeader;

        ControllableLeaderLocator( AtomicBoolean shouldProvideALeader )
        {
            this.shouldProvideALeader = shouldProvideALeader;
        }

        @Override
        public MemberId getLeader() throws NoLeaderFoundException
        {
            if ( shouldProvideALeader.get() )
            {
                return someMember;
            }
            throw new NoLeaderFoundException( "sorry" );
        }

        @Override
        public void registerListener( Listener<MemberId> listener )
        {
            // do nothing
        }

        @Override
        public void unregisterListener( Listener<MemberId> listener )
        {
            // do nothing
        }
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
