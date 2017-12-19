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

import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.logging.Log;

import static java.lang.String.format;

class PersistentSnapshotDownloader implements Runnable
{
    static final String OPERATION_NAME = "download of snapshot";

    private final CommandApplicationProcess applicationProcess;
    private final LeaderLocator leaderLocator;
    private final CoreStateDownloader downloader;
    private final Log log;
    private final TimeoutStrategy.Timeout timeout;
    private volatile State state;
    private volatile boolean keepRunning;

    PersistentSnapshotDownloader( LeaderLocator leaderLocator,
            CommandApplicationProcess applicationProcess, CoreStateDownloader downloader, Log log,
            TimeoutStrategy.Timeout pauseStrategy )
    {
        this.applicationProcess = applicationProcess;
        this.leaderLocator = leaderLocator;
        this.downloader = downloader;
        this.log = log;
        this.timeout = pauseStrategy;
        this.state = State.INITIATED;
        this.keepRunning = true;
    }

    private enum State
    {
        INITIATED,
        RUNNING,
        COMPLETED
    }

    @Override
    public void run()
    {
        if ( !moveToRunningState() )
        {
            return;
        }

        try
        {
            applicationProcess.pauseApplier( OPERATION_NAME );
            while ( keepRunning )
            {
                try
                {
                    downloader.downloadSnapshot( leaderLocator.getLeader() );
                    break;
                }
                catch ( StoreCopyFailedException e )
                {
                    log.error( format( "Failed to download snapshot. Retrying in %s ms.", timeout.getMillis() ), e );
                }
                catch ( NoLeaderFoundException e )
                {
                    log.warn( "No leader found. Retrying in {} ms.", timeout.getMillis() );
                }
                Thread.sleep( timeout.getMillis() );
                timeout.increment();
            }
        }
        catch ( InterruptedException e )
        {
            log.error( "Persistent snapshot downloader was interrupted" );
        }
        finally
        {
            applicationProcess.resumeApplier( OPERATION_NAME );
            state = State.COMPLETED;
        }
    }

    private synchronized boolean moveToRunningState()
    {
        if ( state != State.INITIATED )
        {
            return false;
        }
        else
        {
            state = State.RUNNING;
            return true;
        }
    }

    void stop() throws InterruptedException
    {
        this.keepRunning = false;

        while ( !hasCompleted() )
        {
            Thread.sleep( 100 );
        }
    }

    boolean hasCompleted()
    {
        return state == State.COMPLETED;
    }
}
