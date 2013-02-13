/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.helpers.collection.Iterables;

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
                                           MessageHolder outgoing
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
                                           MessageHolder outgoing
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
                                // Only the first alive server should try elections. Everyone else waits
                                boolean isElector = Iterables.indexOf( context.getClusterContext().getMe(),
                                        context.getHeartbeatContext().getAlive() ) == 0;

                                if ( isElector )
                                {
                                    // Start election process for all roles that are currently unassigned
                                    Iterable<String> rolesRequiringElection = context.getRolesRequiringElection();
                                    for ( String role : rolesRequiringElection )
                                    {
                                        if ( !context.isElectionProcessInProgress( role ) )
                                        {
                                            context.getClusterContext().getLogger( ElectionState.class ).debug(
                                                    "Starting election process for role " + role );

                                            context.startDemotionProcess( role, demoteNode );

                                            // Allow other live nodes to vote which one should take over
                                            for ( URI uri : context.getClusterContext().getConfiguration().getMembers() )
                                            {
                                                if ( !context.getHeartbeatContext().getFailed().contains( uri ) )
                                                {
                                                    // This is a candidate - allow it to vote itself for promotion
                                                    outgoing.offer( Message.to( ElectionMessage.vote, uri, role ) );
                                                }
                                            }
                                            context.getClusterContext()
                                                    .timeouts
                                                    .setTimeout( "election-" + role,
                                                            Message.timeout( ElectionMessage.electionTimeout, message,
                                                                    new ElectionTimeoutData( role, message ) ) );
                                        }
                                        else
                                        {
                                            context.getClusterContext().getLogger( ElectionState.class ).debug(
                                                    "Election already in progress for role " + role );
                                        }
                                    }
                                }
                            }
                            break;
                        }

                        case performRoleElections:
                        {
                            if ( context.getClusterContext().isInCluster() )
                            {
                                // Only the first alive server should try elections. Everyone else waits
                                boolean isElector = Iterables.indexOf( context.getClusterContext().getMe(),
                                        context.getHeartbeatContext().getAlive() ) == 0;

                                if ( isElector )
                                {
                                    // Start election process for all roles
                                    Iterable<ElectionRole> rolesRequiringElection = context.getPossibleRoles();
                                    for ( ElectionRole role : rolesRequiringElection )
                                    {
                                        if ( !context.isElectionProcessInProgress( role.getName() ) )
                                        {
                                            context.getClusterContext().getLogger( ElectionState.class ).debug(
                                                    "Starting election process for role " + role );

                                            context.startElectionProcess( role.getName() );

                                            // Allow other live nodes to vote which one should take over
                                            for ( URI uri : context.getClusterContext().getConfiguration().getMembers() )
                                            {
                                                if ( !context.getHeartbeatContext().getFailed().contains( uri ) )
                                                {
                                                    // This is a candidate - allow it to vote itself for promotion
                                                    outgoing.offer( Message.to( ElectionMessage.vote, uri, role.getName() ) );
                                                }
                                            }
                                            context.getClusterContext()
                                                    .timeouts
                                                    .setTimeout( "election-" + role.getName(),
                                                            Message.timeout( ElectionMessage.electionTimeout, message,
                                                                    new ElectionTimeoutData( role.getName(), message ) ) );
                                        }
                                        else
                                        {
                                            context.getClusterContext().getLogger( ElectionState.class ).debug(
                                                    "Election already in " +
                                                    "progress for role " + role.getName() );
                                        }
                                    }
                                } else
                                {
                                    outgoing.offer( message.setHeader( Message.TO, Iterables.first(
                                            context.getHeartbeatContext().getAlive() ).toString() ) );
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
                                            outgoing.offer( Message.to( ElectionMessage.vote, uri, role ) );
                                        }
                                    }
                                    context.getClusterContext()
                                            .timeouts
                                            .setTimeout( "election-" + role, Message.timeout( ElectionMessage
                                                    .electionTimeout, message, new ElectionTimeoutData( role, message ) ) );
                                }
                            }
                            break;
                        }

                        case vote:
                        {
                            String role = message.getPayload();
                            outgoing.offer( Message.respond( ElectionMessage.voted, message,
                                    new ElectionMessage.VotedData( role, context.getCredentialsForRole( role ) ) ) );
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
                                            winner + " as " + data.getRole() );

                                    // Broadcast this
                                    ClusterMessage.ConfigurationChangeState configurationChangeState = new
                                            ClusterMessage.ConfigurationChangeState();
                                    configurationChangeState.elected( data.getRole(), winner );
                                    outgoing.offer( Message.internal( ProposerMessage.propose,
                                            configurationChangeState ) );
                                }
                                else
                                {
                                    context.getClusterContext().getLogger( ElectionState.class ).warn( "Election " +
                                            "could not pick a winner" );
                                }
                                context.getClusterContext().timeouts.cancelTimeout( "election-" + data.getRole() );
                            }
                            break;
                        }

                        case electionTimeout:
                        {
                            // Election failed - try again
                            ElectionTimeoutData electionTimeoutData = (ElectionTimeoutData) message.getPayload();
                            context.getClusterContext().getLogger( ElectionState.class ).warn( String.format(
                                    "Election timed out for '%s'- trying again", electionTimeoutData.getRole() ));
                            context.cancelElection( electionTimeoutData.getRole() );
                            outgoing.offer( electionTimeoutData.getMessage() );
                            break;
                        }

                        case leave:
                        {
                            return start;
                        }
                    }

                    return this;
                }
            };


    private static class ElectionTimeoutData
    {
        private final String role;
        private final Message message;

        private ElectionTimeoutData( String role, Message message )
        {
            this.role = role;
            this.message = message;
        }

        public String getRole()
        {
            return role;
        }

        public Message getMessage()
        {
            return message;
        }
    }
}
