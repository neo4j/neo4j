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
package org.neo4j.causalclustering.core.state;

import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.ConsensusOutcome;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateDownloader;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftMessageHandler implements Inbound.MessageHandler<RaftMessages.ClusterIdAwareMessage>
{
    private final LocalDatabase localDatabase;
    private final Log log;
    private final RaftMachine raftMachine;
    private final CoreStateDownloader downloader;
    private final CommandApplicationProcess applicationProcess;

    private ClusterId boundClusterId;

    public RaftMessageHandler( LocalDatabase localDatabase, LogProvider logProvider,
            RaftMachine raftMachine, CoreStateDownloader downloader,
            CommandApplicationProcess applicationProcess )
    {
        this.localDatabase = localDatabase;
        this.log = logProvider.getLog( getClass() );
        this.raftMachine = raftMachine;
        this.downloader = downloader;
        this.applicationProcess = applicationProcess;
    }

    public synchronized void handle( RaftMessages.ClusterIdAwareMessage clusterIdAwareMessage )
    {
        if ( boundClusterId == null )
        {
            return;
        }

        ClusterId msgClusterId = clusterIdAwareMessage.clusterId();
        if ( msgClusterId.equals( boundClusterId ) )
        {
            try
            {
                ConsensusOutcome outcome = raftMachine.handle( clusterIdAwareMessage.message() );
                if ( outcome.needsFreshSnapshot() )
                {
                    downloadSnapshot( clusterIdAwareMessage.message().from() );
                }
                else
                {
                    notifyCommitted( outcome.getCommitIndex() );
                }
            }
            catch ( Throwable e )
            {
                log.error( "Error handling message", e );
                raftMachine.panic();
                localDatabase.panic( e );
            }
        }
        else
        {
            log.info( "Discarding message[%s] owing to mismatched clusterId. Expected: %s, Encountered: %s",
                    clusterIdAwareMessage.message(), boundClusterId, msgClusterId );
        }
    }

    synchronized void start( ClusterId clusterId ) throws TimeoutException
    {
        boundClusterId = clusterId;
    }

    synchronized void stop()
    {
        boundClusterId = null;
    }

    private void notifyCommitted( long commitIndex )
    {
        applicationProcess.notifyCommitted( commitIndex );
    }

    /**
     * Attempts to download a fresh snapshot from another core instance.
     *
     * @param source The source address to attempt a download of a snapshot from.
     */
    private void downloadSnapshot( MemberId source ) throws Throwable
    {
        try
        {
            downloader.downloadSnapshot( source );
        }
        catch ( StoreCopyFailedException e )
        {
            log.error( "Failed to download snapshot", e );
        }
    }
}
