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
package org.neo4j.causalclustering.core.consensus;

import java.util.Objects;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.Inbound;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftMessagesPreHandler implements Inbound.MessageHandler<RaftMessages.ClusterIdAwareMessage>
{
    private final Inbound.MessageHandler<RaftMessages.ClusterIdAwareMessage> delegateHandler;
    private final ElectionTiming electionTiming;
    private final RaftMachine raftMachine;
    private final Log log;
    private volatile ClusterId boundClusterId;

    public RaftMessagesPreHandler( Inbound.MessageHandler<RaftMessages.ClusterIdAwareMessage> delegateHandler, ElectionTiming electionTiming,
            RaftMachine raftMachine, LogProvider logProvider )
    {
        this.delegateHandler = delegateHandler;
        this.electionTiming = electionTiming;
        this.raftMachine = raftMachine;
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized void start( ClusterId clusterId )
    {
        boundClusterId = clusterId;
    }

    public synchronized void stop()
    {
        boundClusterId = null;
    }

    @Override
    public void handle( RaftMessages.ClusterIdAwareMessage message )
    {
        if ( Objects.isNull( boundClusterId ) )
        {
            log.debug( "This pre handler has been stopped, dropping the message: %s", message.message() );
        }
        else if ( !Objects.equals( message.clusterId(), boundClusterId ) )
        {
            log.info( "Discarding message[%s] owing to mismatched clusterId. Expected: %s, Encountered: %s",
                    message.message(), boundClusterId, message.clusterId() );
        }
        else
        {
            handleTimeouts( message );

            delegateHandler.handle( message );
        }
    }

    private void handleTimeouts( RaftMessages.ClusterIdAwareMessage message )
    {
        if ( shouldRenewElectionTimeout( message.message() ) )
        {
            electionTiming.renewElection();
        }
    }

    // TODO replace with visitor pattern
    private boolean shouldRenewElectionTimeout( RaftMessages.RaftMessage message )
    {
        switch ( message.type() )
        {
        case HEARTBEAT:
            RaftMessages.Heartbeat heartbeat = (RaftMessages.Heartbeat) message;
            return heartbeat.leaderTerm() >= raftMachine.term();
        case APPEND_ENTRIES_REQUEST:
            RaftMessages.AppendEntries.Request request = (RaftMessages.AppendEntries.Request) message;
            return request.leaderTerm() >= raftMachine.term();
        default:
            return false;
        }
    }
}
