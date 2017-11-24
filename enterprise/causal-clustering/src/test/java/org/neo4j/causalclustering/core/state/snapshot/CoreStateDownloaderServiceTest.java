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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloaderService.OPERATION_NAME;

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
                        logProvider( log ) );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( someMember );
        coreStateDownloaderService.scheduleDownload( leaderLocator );
        Predicates.await( () -> {
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

        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        verify( coreStateDownloader, times( 1 ) ).downloadSnapshot( any() );
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
