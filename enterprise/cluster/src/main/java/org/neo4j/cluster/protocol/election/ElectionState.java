/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cluster.protocol.election;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;

import static org.neo4j.helpers.collection.Iterables.firstOrNull;

/**
 * State machine that implements the {@link Election} API.
 */
public enum ElectionState
        implements State<ElectionContext, ElectionMessage>
{
    start
            {
                @Override
                public ElectionState handle( ElectionContext context,
                                           Message<ElectionMessage> message,
                                           MessageHolder outgoing
                )
                {
                    if ( message.getMessageType() == ElectionMessage.created )
                    {
                        context.created();
                        return election;
                    }
                    else if ( message.getMessageType() == ElectionMessage.join )
                    {
                        return election;
                    }

                    return this;
                }
            },

    election
            {
                @Override
                public ElectionState handle( ElectionContext context,
                                           Message<ElectionMessage> message,
                                           MessageHolder outgoing
                )
                {
                    Log log = context.getLog( ElectionState.class );
                    switch ( message.getMessageType() )
                    {
                        case demote:
                        {
                            if ( !context.electionOk() )
                            {
                                log.warn( "Context says election is not OK to proceed. " +
                                        "Failed instances are: " +
                                        context.getFailed() +
                                        ", cluster members are: " +
                                        context.getMembers()  );
                                break;
                            }

                            InstanceId demoteNode = message.getPayload();
                            // TODO  Could perhaps be done better?
                            context.nodeFailed( demoteNode );
                            if ( context.isInCluster() )
                            {
                                // Only the first alive server should try elections. Everyone else waits
                                List<InstanceId> aliveInstances = Iterables.asList(context.getAlive());
                                Collections.sort( aliveInstances );
                                boolean isElector = aliveInstances.indexOf( context.getMyId() ) == 0;

                                if ( isElector )
                                {
                                    log.debug( "I (" + context.getMyId() +
                                            ") am the elector, executing the election" );
                                    // Start election process for all roles that are currently unassigned
                                    Iterable<String> rolesRequiringElection = context.getRolesRequiringElection();
                                    for ( String role : rolesRequiringElection )
                                    {
                                        if ( !context.isElectionProcessInProgress( role ) )
                                        {
                                            log.debug( "Starting election process for role " + role );

                                            context.startElectionProcess( role );

                                            // Allow other live nodes to vote which one should take over
                                            for ( Map.Entry<InstanceId, URI> server : context.getMembers().entrySet() )
                                            {
                                                if ( !context.getFailed().contains( server.getKey() ) )
                                                {
                                                    // This is a candidate - allow it to vote itself for promotion
                                                    outgoing.offer( Message.to( ElectionMessage.vote, server.getValue(),
                                                            context.voteRequestForRole( new ElectionRole( role ) ) ) );
                                                }
                                            }
                                            context.setTimeout( "election-" + role,
                                                    Message.timeout( ElectionMessage.electionTimeout, message,
                                                            new ElectionTimeoutData( role, message ) ) );
                                        }
                                        else
                                        {
                                            log.debug( "Election already in progress for role " + role );
                                        }
                                    }
                                }
                            }
                            break;
                        }

                        case performRoleElections:
                        {
                            if ( !context.electionOk() )
                            {
                                log.warn( "Context says election is not OK to proceed. " +
                                        "Failed instances are: " +
                                        context.getFailed() +
                                        ", cluster members are: " +
                                        context.getMembers()  );
                                break;
                            }
                            if ( context.isInCluster() )
                            {
                                boolean isElector = context.isElector();

                                if ( isElector )
                                {
                                    context.getLog( ElectionState.class ).info( "I am the elector, doing election..." );
                                    // Start election process for all roles
                                    Iterable<ElectionRole> rolesRequiringElection = context.getPossibleRoles();
                                    for ( ElectionRole role : rolesRequiringElection )
                                    {
                                        String roleName = role.getName();
                                        if ( !context.isElectionProcessInProgress( roleName ) )
                                        {
                                            context.getLog( ElectionState.class ).debug(
                                                    "Starting election process for role " + roleName );

                                            context.startElectionProcess( roleName );

                                            boolean sentSome = false;
                                            // Allow other live nodes to vote which one should take over
                                            for ( Map.Entry<InstanceId, URI> server : context.getMembers().entrySet() )
                                            {
                                                /*
                                                 * Skip dead nodes and the current role holder. Dead nodes are not
                                                 * candidates anyway and the current role holder will be asked last,
                                                 * after everyone else has cast votes.
                                                 */
                                                if ( !context.isFailed( server.getKey() ) &&
                                                        !server.getKey().equals( context.getElected( roleName ) ) )
                                                {
                                                    // This is a candidate - allow it to vote itself for promotion
                                                    outgoing.offer( Message.to( ElectionMessage.vote,
                                                            server.getValue(), context.voteRequestForRole( role ) ) );
                                                    sentSome = true;
                                                }
                                            }
                                            if ( !sentSome )
                                            {
                                                /*
                                                 * If we didn't send any messages, we are the only non-failed cluster
                                                 * member and probably (not necessarily) hold the role, though that
                                                 * doesn't matter. So we ask ourselves to vote, if we didn't above.
                                                 * In this case, no timeout is required, because no messages are
                                                 * expected. If we are indeed the role holder, then we'll cast our
                                                 * vote as a response to this message, which will complete the election.
                                                 */
                                                outgoing.offer( Message.internal( ElectionMessage.vote,
                                                        context.voteRequestForRole( new ElectionRole( roleName ) ) ) );
                                            }
                                            else
                                            {
                                                context.setTimeout( "election-" + roleName,
                                                        Message.timeout( ElectionMessage.electionTimeout, message,
                                                                new ElectionTimeoutData( roleName, message ) ) );
                                            }
                                        }
                                        else
                                        {
                                            log.debug( "Election already in progress for role " + roleName );
                                        }
                                    }
                                }
                                else
                                {
                                    /*
                                     * We take alive instances as determined by suspicions and remove those that are
                                     * marked as failed in the failed set. This is done so that an instance which
                                     * just joined can use the failed set provided in the configuration response to
                                     * correctly determine the instances that are failed and skip them.
                                     * Basically, this is to solve an issue where if an instance joins and is the
                                     * lowest numbered alive but not overall will not try to get the failed lower
                                     * numbered one to do elections.
                                     */
                                    Set<InstanceId> aliveInstances = Iterables.asSet( context.getAlive() );
                                    aliveInstances.removeAll( context.getFailed() );
                                    List<InstanceId> adjustedAlive = Iterables.asList( aliveInstances );
                                    Collections.sort( adjustedAlive );

                                    context.getLog( ElectionState.class ).info( "I am NOT the elector, sending to " + adjustedAlive );
                                    outgoing.offer( message.setHeader( Message.HEADER_TO,
                                            context.getUriForId( firstOrNull( adjustedAlive ) ).toString() ) );
                                }
                            }
                            break;
                        }

                        case vote:
                        {
                            Object request = message.getPayload();

                            ElectionContext.VoteRequest voteRequest = (ElectionContext.VoteRequest) request;
                            outgoing.offer( Message.respond( ElectionMessage.voted, message,
                                    new ElectionMessage.VersionedVotedData( voteRequest.getRole(), context.getMyId(),
                                            context.getCredentialsForRole( voteRequest.getRole() ),
                                            voteRequest.getVersion() ) ) );
                            break;
                        }

                        case voted:
                        {
                            ElectionMessage.VotedData data = message.getPayload();
                            long version = -1;
                            if ( data instanceof ElectionMessage.VersionedVotedData )
                            {
                                version = ((ElectionMessage.VersionedVotedData) data).getVersion();
                            }
                            boolean accepted =
                                    context.voted( data.getRole(), data.getInstanceId(), data.getElectionCredentials(),
                                            version );

                            String voter = message.hasHeader( Message.HEADER_FROM ) ? message.getHeader( Message.HEADER_FROM ) : "I";
                            log.debug( voter + " voted " + data + " which i " +
                                    ( accepted ? "accepted" : "did not accept" ) );

                            if ( !accepted )
                            {
                                break;
                            }

                            /*
                             * This is the URI of the current role holder and, yes, it could very well be null. However
                             * we don't really care. If it is null then the election would not have sent one vote
                             * request less than needed (i.e. ask the master last) since, well, it doesn't exist. So
                             * the immediate effect is that the else (which checks for null) will never be called.
                             */
                            InstanceId currentElected = context.getElected( data.getRole() );

                            if ( context.getVoteCount( data.getRole() ) == context.getNeededVoteCount() )
                            {
                                // We have all votes now
                                InstanceId winner = context.getElectionWinner( data.getRole() );

                                context.cancelTimeout( "election-" + data.getRole() );
                                context.forgetElection( data.getRole() );

                                if ( winner != null )
                                {
                                    log.debug( "Elected " + winner + " as " + data.getRole() );

                                    // Broadcast this
                                    ClusterMessage.VersionedConfigurationStateChange configurationChangeState =
                                            context.newConfigurationStateChange();
                                    configurationChangeState.elected( data.getRole(), winner );

                                    outgoing.offer( Message.internal( AtomicBroadcastMessage.broadcast,
                                            configurationChangeState ) );
                                }
                                else
                                {
                                    log.warn( "Election could not pick a winner" );
                                    if ( currentElected != null )
                                    {
                                        // Someone had the role and doesn't anymore. Broadcast this
                                        ClusterMessage.ConfigurationChangeState configurationChangeState =
                                                new ClusterMessage.ConfigurationChangeState();
                                        configurationChangeState.unelected( data.getRole(), currentElected );
                                        outgoing.offer( Message.internal( ProposerMessage.propose,
                                                configurationChangeState ) );
                                    }
                                }
                            }
                            else if ( context.getVoteCount( data.getRole() ) == context.getNeededVoteCount() - 1 &&
                                    currentElected != null &&
                                    !context.hasCurrentlyElectedVoted( data.getRole(), currentElected ) )
                            {
                                // Missing one vote, the one from the current role holder
                                outgoing.offer( Message.to( ElectionMessage.vote,
                                        context.getUriForId( currentElected ),
                                        context.voteRequestForRole( new ElectionRole( data.getRole() ) ) ) );
                            }
                            break;
                        }

                        case electionTimeout:
                        {
                            // Election failed - try again
                            ElectionTimeoutData electionTimeoutData = message.getPayload();
                            log.warn( String.format(
                                    "Election timed out for '%s'- trying again", electionTimeoutData.getRole() ) );
                            context.forgetElection( electionTimeoutData.getRole() );
                            outgoing.offer( electionTimeoutData.getMessage() );
                            break;
                        }

                        case leave:
                        {
                            return start;
                        }

                        default:
                            break;
                    }

                    return this;
                }
            };

    public static class ElectionTimeoutData
    {
        private final String role;
        private final Message message;

        public ElectionTimeoutData( String role, Message message )
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
