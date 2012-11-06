/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.cluster.protocol.election;

import java.net.URI;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.statemachine.State;

/**
 * State machine that implements the {@link Election} API.
 */
public enum ElectionState
        implements State<ElectionContext, ElectionMessage>
{
    start
            {
                @Override
                public State<?, ?> handle( ElectionContext context,
                                           Message<ElectionMessage> message,
                                           MessageProcessor outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case created:
                        {
                            context.created();
                            return election;
                        }

                        case join:
                        {
                            return election;
                        }
                    }

                    return this;
                }
            },

    election
            {
                @Override
                public State<?, ?> handle( ElectionContext context,
                                           Message<ElectionMessage> message,
                                           MessageProcessor outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case demote:
                        {
                            URI demoteNode = message.getPayload();
                            // TODO  Could perhaps be done better?
                            context.nodeFailed( demoteNode );
                            if ( context.getClusterContext().isInCluster() )
                            {
                                int remainingDelays;
                                if ( message.hasHeader( "delays" ) )
                                {
                                    // Check how many more times we should delay before proceeding
                                    remainingDelays = Integer.parseInt( message.getHeader( "delays" ) );
                                }
                                else
                                {
                                    // Should we delay this so that someone else can give it a go?
                                    remainingDelays = context.getClusterContext().getConfiguration().getMembers()
                                            .indexOf( context.getClusterContext().getMe() );
                                    if ( remainingDelays > 0 )
                                    {
                                        context.getClusterContext().getLogger( ElectionState.class ).debug( "Delay " +
                                                "demotion of " + demoteNode
                                                .toString() + " " + remainingDelays + " times" );
                                    }
                                }

                                if ( remainingDelays > 0 )
                                {
                                    context.getClusterContext()
                                            .timeouts
                                            .setTimeout( "delayed-demote-" + demoteNode.toString(),
                                                    Message.timeout( ElectionMessage.demote, message,
                                                            demoteNode ).setHeader( "delays",
                                                            (remainingDelays - 1) + "" ) );
                                }
                                else
                                // Start election process for all roles that are currently unassigned
                                {
                                    Iterable<String> rolesRequiringElection = context.getRolesRequiringElection();
                                    for ( String role : rolesRequiringElection )
                                    {
                                        if ( !context.isElectionProcessInProgress( role ) )
                                        {
                                            context.getClusterContext().getLogger( ElectionState.class ).debug(
                                                    "Starting election process" +
                                                    " for role " + role );

                                            int voterCount = 0;
                                            context.startDemotionProcess( role, demoteNode );

                                            // Allow other live nodes to vote which one should take over
                                            for ( URI uri : context.getClusterContext().getConfiguration().getMembers
                                                    () )
                                            {
                                                if ( !context.getHeartbeatContext().getFailed().contains( uri ) )
                                                {
                                                    // This is a candidate - allow it to vote itself for promotion
                                                    outgoing.process( Message.to( ElectionMessage.vote, uri, role ) );
                                                    voterCount++;
                                                }
                                            }
                                            context.getClusterContext()
                                                    .timeouts
                                                    .setTimeout( "election-" + role,
                                                            Message.timeout( ElectionMessage.electionTimeout, message,
                                                                    role ) );
                                        }
                                        else
                                        {
                                            context.getClusterContext().getLogger( ElectionState.class ).debug(
                                                    "Election already in " +
                                                    "progress for role " + role );
                                        }
                                    }
                                }
                            }
                            break;
                        }

                        case promote:
                        {
                            Object[] args = message.<Object[]>getPayload();
                            URI promoteNode = (URI) args[0];
                            String role = (String) args[1];

                            if ( context.getClusterContext().isInCluster() )
                            {
                                // Start election process for coordinator role
                                if ( !context.isElectionProcessInProgress( role ) )
                                {
                                    context.startPromotionProcess( role, promoteNode );

                                    // Allow other live nodes to vote which one should take over
                                    for ( URI uri : context.getClusterContext().getConfiguration().getMembers() )
                                    {
                                        if ( !context.getHeartbeatContext().getFailed().contains( uri ) )
                                        {
                                            // This is a candidate - allow it to vote itself for promotion
                                            outgoing.process( Message.to( ElectionMessage.vote, uri, role ) );
                                        }
                                    }
                                    context.getClusterContext()
                                            .timeouts
                                            .setTimeout( "election-" + role, Message.timeout( ElectionMessage
                                                    .electionTimeout, message, role ) );
                                }
                            }
                            break;
                        }

                        case vote:
                        {
                            String role = message.getPayload();
                            outgoing.process( Message.respond( ElectionMessage.voted, message,
                                    new ElectionMessage.VotedData( role, context
                                            .getCredentialsForRole( role ) ) ) );
                            break;
                        }

                        case voted:
                        {
                            ElectionMessage.VotedData data = message.getPayload();
                            context.voted( data.getRole(), new URI( message.getHeader( Message.FROM ) ),
                                    data.getVoteCredentials() );

                            if ( context.getVoteCount( data.getRole() ) == context.getNeededVoteCount() )
                            {
                                // We have all votes now
                                URI winner = context.getElectionWinner( data.getRole() );

                                if ( winner != null )
                                {
                                    context.getClusterContext().getLogger( ElectionState.class ).debug( "Elected " +
                                            winner + " as " +
                                            data.getRole() );

                                    // Broadcast this
                                    ClusterMessage.ConfigurationChangeState configurationChangeState = new
                                            ClusterMessage.ConfigurationChangeState();
                                    configurationChangeState.elected( data.getRole(), winner );
                                    outgoing.process( Message.internal( ProposerMessage.propose,
                                            configurationChangeState ) );
                                }
                                else
                                {
                                    context.getClusterContext().getLogger( ElectionState.class ).warn( "Election " +
                                            "could not " +
                                            "pick a " +
                                            "winner" );
                                }

                                context.getClusterContext().timeouts.cancelTimeout( "election-" + data.getRole() );
                            }
                            break;
                        }

                        case electionTimeout:
                        {
                            // Something was lost
                            context.getClusterContext().getLogger( ElectionState.class ).warn( "Election timed out" );
                            context.cancelElection( (String) message.getPayload() );
                            break;
                        }

                        case leave:
                        {
                            return start;
                        }
                    }

                    return this;
                }
            }
}
