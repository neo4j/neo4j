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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.helpers.collection.Iterables;

import static org.neo4j.cluster.com.message.Message.internal;
import static org.neo4j.cluster.com.message.Message.respond;
import static org.neo4j.cluster.com.message.Message.timeout;
import static org.neo4j.cluster.com.message.Message.to;
import static org.neo4j.helpers.collection.Iterables.count;

/**
 * State machine for the Cluster API
 *
 * @see Cluster
 * @see ClusterMessage
 */
public enum ClusterState
        implements State<ClusterContext, ClusterMessage>
{
    start
            {
                @Override
                public State<?, ?> handle( ClusterContext context, Message<ClusterMessage> message,
                                           MessageHolder outgoing ) throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case addClusterListener:
                        {
                            context.addClusterListener( message.<ClusterListener>getPayload() );

                            break;
                        }

                        case removeClusterListener:
                        {
                            context.removeClusterListener( message.<ClusterListener>getPayload() );

                            break;
                        }

                        case create:
                        {
                            String name = message.getPayload();
                            context.getLog( ClusterState.class ).info( "Creating cluster: " + name );
                            context.created( name );
                            return entered;
                        }

                        case join:
                        {
                            // Send configuration request to all instances
                            Object[] args = message.<Object[]>getPayload();
                            String name = ( String ) args[0];
                            URI[] clusterInstanceUris = ( URI[] ) args[1];
                            context.joining( name, Iterables.<URI,URI>iterable( clusterInstanceUris ) );

                            for ( URI potentialClusterInstanceUri : clusterInstanceUris )
                            {
                                outgoing.offer( to( ClusterMessage.configurationRequest,
                                        potentialClusterInstanceUri,
                                        new ClusterMessage.ConfigurationRequestState( context.getMyId(), context.boundAt() ) ) );
                            }
                            context.setTimeout( "discovery",
                                    timeout( ClusterMessage.configurationTimeout, message,
                                            new ClusterMessage.ConfigurationTimeoutState(
                                                    /*
                                                     * The time when this becomes relevant is if indeed there are
                                                     * other instances present in the configuration. If there aren't
                                                     * we won't wait for them anyway and only this delay prevents us
                                                     * from going ahead and creating the cluster. We still wait a bit
                                                     * though because even if we don't have them configured they still
                                                     * might contact us.
                                                     * If, on the other hand, we have some configured, then we won't
                                                     * startup anyway until half are available. So this delay doesn't
                                                     * enter into it anyway.
                                                     * In summary, this offers no upside if there are configured
                                                     * instances
                                                     * and causes unnecessary delay if we are supposed to go ahead and
                                                     * create the cluster.
                                                     */
                                                    1 ) ) );
                            return discovery;
                        }
                    }
                    return this;
                }
            },

    discovery
            {
                @Override
                public State<?, ?> handle( ClusterContext context, Message<ClusterMessage> message,
                                           MessageHolder outgoing ) throws Throwable
                {
                    List<ClusterMessage.ConfigurationRequestState> discoveredInstances = context.getDiscoveredInstances();
                    switch ( message.getMessageType() )
                    {
                        case configurationResponse:
                        {
                            context.cancelTimeout( "discovery" );

                            ClusterMessage.ConfigurationResponseState state = message.getPayload();

                            context.getLog( ClusterState.class ).info( "Joining cluster " + state.getClusterName() );
                            if ( !context.getConfiguration().getName().equals( state.getClusterName() ) )
                            {
                                context.getLog( ClusterState.class ).warn( "Joined cluster name is different than " +
                                        "the one configured. Expected " + context.getConfiguration().getName() +
                                        ", got " + state.getClusterName() + "." );
                            }

                            HashMap<InstanceId, URI> memberList = new HashMap<InstanceId, URI>( state.getMembers() );
                            context.discoveredLastReceivedInstanceId( state.getLatestReceivedInstanceId().getId() );

                            context.acquiredConfiguration( memberList, state.getRoles() );

                            if ( !memberList.containsKey( context.getMyId() ) ||
                                    !memberList.get( context.getMyId() ).equals( context.boundAt() ) )
                            {
                                context.getLog( ClusterState.class ).info( String.format( "%s joining:%s, " +
                                        "last delivered:%d", context.getMyId().toString(),
                                        context.getConfiguration().toString(),
                                        state.getLatestReceivedInstanceId().getId() ) );

                                ClusterMessage.ConfigurationChangeState newState = new ClusterMessage.ConfigurationChangeState();
                                newState.join(context.getMyId(), context.boundAt());

                                // Let the coordinator propose this if possible
                                InstanceId coordinator = state.getRoles().get( ClusterConfiguration.COORDINATOR );
                                if ( coordinator != null )
                                {
                                    URI coordinatorUri = context.getConfiguration().getUriForId( coordinator );
                                    outgoing.offer( to( ProposerMessage.propose, coordinatorUri, newState ) );
                                }
                                else
                                {
                                    outgoing.offer( to( ProposerMessage.propose, new URI( message.getHeader(
                                            Message.FROM ) ), newState ) );
                                }

                                context.getLog( ClusterState.class ).debug( "Setup join timeout for " + message
                                        .getHeader( Message.CONVERSATION_ID ) );
                                context.setTimeout( "join", timeout( ClusterMessage.joiningTimeout, message,
                                        new URI( message.getHeader( Message.FROM ) ) ) );

                                return joining;
                            }
                            else
                            {
                                // Already in (probably due to crash of this server previously), go to entered state
                                context.joined();
                                outgoing.offer( internal( ClusterMessage.joinResponse, context.getConfiguration() ) );

                                return entered;
                            }
                        }

                        case configurationTimeout:
                        {
                            if ( context.hasJoinBeenDenied() )
                            {
                                outgoing.offer( internal( ClusterMessage.joinFailure,
                                        new ClusterEntryDeniedException( context.getMyId(),
                                                context.getJoinDeniedConfigurationResponseState() ) ) );
                                return start;
                            }
                            ClusterMessage.ConfigurationTimeoutState state = message.getPayload();
                            if ( state.getRemainingPings() > 0 )
                            {
                                // Send out requests again
                                for ( URI potentialClusterInstanceUri : context.getJoiningInstances() )
                                {
                                    outgoing.offer( to( ClusterMessage.configurationRequest,
                                            potentialClusterInstanceUri,
                                            new ClusterMessage.ConfigurationRequestState(
                                                    context.getMyId(), context.boundAt() ) ) );
                                }
                                context.setTimeout( "join",
                                        timeout( ClusterMessage.configurationTimeout, message,
                                                new ClusterMessage.ConfigurationTimeoutState(
                                                        state.getRemainingPings() - 1 ) ) );
                            }
                            else
                            {
                                /*
                                 * No configuration responses. Check if we picked up any other instances' requests during this phase.
                                 * If we did, or we are the only instance in the configuration we can go ahead and try to start the
                                 * cluster.
                                 */
                                if ( !discoveredInstances.isEmpty() || count( context.getJoiningInstances() ) == 1 )
                                {
                                    Collections.sort( discoveredInstances );
                                    /*
                                     * The assumption here is that the lowest in the list of discovered instances
                                     * will create the cluster. Keep in mind that this is run on all instances so
                                     * everyone will pick the same one.
                                     * If the one picked up is configured to not init a cluster then the timeout
                                     * set in else{} will take care of that.
                                     * We also start the cluster if we are the only configured instance. joiningInstances
                                     * does not contain us, ever.
                                     */
                                    ClusterMessage.ConfigurationRequestState ourRequestState =
                                            new ClusterMessage.ConfigurationRequestState(context.getMyId(), context.boundAt());
                                    // No one to join with
                                    boolean imAlone =
                                            count(context.getJoiningInstances()) == 1
                                            && discoveredInstances.contains(ourRequestState)
                                            && discoveredInstances.size() == 1;
                                    // Enough instances discovered (half or more - i don't count myself here)
                                    boolean haveDiscoveredMajority =
                                            discoveredInstances.size() >= Iterables.count( context.getJoiningInstances() );
                                    // I am supposed to create the cluster (i am before the first in the list of the discovered instances)
                                    boolean wantToStartCluster =
                                            !discoveredInstances.isEmpty()
                                            && discoveredInstances.get( 0 ).getJoiningId().compareTo(context.getMyId() ) >= 0;
                                    if ( imAlone || haveDiscoveredMajority && wantToStartCluster )
                                    {
                                        discoveredInstances.clear();

                                        // I'm supposed to create the cluster - fail the join
                                        outgoing.offer( internal( ClusterMessage.joinFailure,
                                                new TimeoutException(
                                                        "Join failed, timeout waiting for configuration" ) ) );
                                        return start;
                                    }
                                    else
                                    {
                                        discoveredInstances.clear();

                                        // Someone else is supposed to create the cluster - restart the join discovery
                                        for ( URI potentialClusterInstanceUri : context.getJoiningInstances() )
                                        {
                                            outgoing.offer( to( ClusterMessage.configurationRequest,
                                                    potentialClusterInstanceUri,
                                                    new ClusterMessage.ConfigurationRequestState( context.getMyId(),
                                                            context.boundAt() ) ) );
                                        }
                                        context.setTimeout( "discovery",
                                                timeout( ClusterMessage.configurationTimeout, message,
                                                        new ClusterMessage.ConfigurationTimeoutState( 4 ) ) );
                                    }
                                }
                                else
                                {
                                     context.setTimeout( "join",
                                             timeout( ClusterMessage.configurationTimeout, message,
                                                     new ClusterMessage.ConfigurationTimeoutState( 4 ) ) );
                                }
                            }

                            return this;
                        }

                        case configurationRequest:
                        {
                            // We're listening for existing clusters, but if all instances start up at the same time
                            // and look for each other, this allows us to pick that up
                            ClusterMessage.ConfigurationRequestState configurationRequested = message.getPayload();
                            configurationRequested = new ClusterMessage.ConfigurationRequestState( configurationRequested.getJoiningId(), URI.create(message.getHeader( Message.FROM ) ));

                            if ( !discoveredInstances.contains( configurationRequested ))
                            {
                                for ( ClusterMessage.ConfigurationRequestState discoveredInstance :
                                        discoveredInstances )
                                {
                                    if ( discoveredInstance.getJoiningId().equals( configurationRequested.getJoiningId() ) )
                                    {
                                        // we are done
                                        outgoing.offer( internal( ClusterMessage.joinFailure,
                                                new IllegalStateException( String.format(
                                                        "Failed to join cluster because I saw two instances with the " +
                                                                "same ServerId. " +
                                                                "One is %s. The other is %s", discoveredInstance,
                                                        configurationRequested ) ) ) );
                                        return start;
                                    }
                                }
                                discoveredInstances.add( configurationRequested );
                            }
                            break;
                        }

                        case joinDenied:
                        {
//                            outgoing.offer( internal( ClusterMessage.joinFailure,
//                                    new ClusterEntryDeniedException( context.me, context.configuration ) ) );
//                            return start;
                            context.joinDenied( (ClusterMessage.ConfigurationResponseState) message.getPayload() );
                            return this;
                        }
                    }

                    return this;
                }
            },

    joining
            {
                @Override
                public State<?, ?> handle( ClusterContext context,
                                           Message<ClusterMessage> message,
                                           MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case configurationChanged:
                        {
                            ClusterMessage.ConfigurationChangeState state = message.getPayload();

                            if ( context.getMyId().equals( state.getJoin() ) )
                            {
                                context.cancelTimeout( "join" );

                                context.joined();
                                outgoing.offer( message.copyHeadersTo( internal( ClusterMessage.joinResponse, context.getConfiguration() ) ) );
                                return entered;
                            }
                            else
                            {
                                state.apply( context );
                                return this;
                            }
                        }

                        case joiningTimeout:
                        {
                            context.getLog( ClusterState.class ).info( "Join timeout for " + message.getHeader(
                                    Message.CONVERSATION_ID ) );

                            if ( context.hasJoinBeenDenied() )
                            {
                                outgoing.offer( internal( ClusterMessage.joinFailure,
                                    new ClusterEntryDeniedException( context.getMyId(),
                                            context.getJoinDeniedConfigurationResponseState() ) ) );
                                return start;
                            }

                            // Go back to requesting configurations from potential members
                            for ( URI potentialClusterInstanceUri : context.getJoiningInstances() )
                            {
                                outgoing.offer( to( ClusterMessage.configurationRequest,
                                        potentialClusterInstanceUri,
                                        new ClusterMessage.ConfigurationRequestState( context.getMyId(), context.boundAt() ) ) );
                            }
                            context.setTimeout( "discovery",
                                    timeout( ClusterMessage.configurationTimeout, message,
                                            new ClusterMessage.ConfigurationTimeoutState( 4 ) ) );

                            return discovery;
                        }

                        case joinFailure:
                        {
                            // This causes an exception from the join() method
                            return start;
                        }
                    }

                    return this;
                }
            },

    entered
            {
                @Override
                public State<?, ?> handle( ClusterContext context, Message<ClusterMessage> message,
                                           MessageHolder outgoing ) throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case addClusterListener:
                        {
                            context.addClusterListener( message.<ClusterListener>getPayload() );

                            break;
                        }

                        case removeClusterListener:
                        {
                            context.removeClusterListener( message.<ClusterListener>getPayload() );

                            break;
                        }

                        case configurationRequest:
                        {
                            ClusterMessage.ConfigurationRequestState request = message.getPayload();
                            request = new ClusterMessage.ConfigurationRequestState( request.getJoiningId(), URI.create(message.getHeader( Message.FROM ) ));

                            InstanceId joiningId = request.getJoiningId();
                            URI joiningUri = request.getJoiningUri();
                            boolean isInCluster = context.getMembers().containsKey( joiningId );
                            boolean isCurrentlyAlive = context.isCurrentlyAlive(joiningId);
                            boolean messageComesFromSameHost = request.getJoiningId().equals( context.getMyId() );
                            boolean otherInstanceJoiningWithSameId = context.isInstanceJoiningFromDifferentUri(
                                    joiningId, joiningUri );
                            boolean isFromSameURIAsTheOneWeAlreadyKnow = context.getUriForId( joiningId ) != null &&
                                    context.getUriForId( joiningId ).equals( joiningUri );

                            boolean somethingIsWrong =
                                    ( isInCluster && !messageComesFromSameHost && isCurrentlyAlive && !isFromSameURIAsTheOneWeAlreadyKnow )
                                            || otherInstanceJoiningWithSameId ;

                            if ( somethingIsWrong )
                            {
                                if(otherInstanceJoiningWithSameId)
                                {
                                    context.getLog( ClusterState.class ).info( "Denying entry to instance " + joiningId + " because another instance is currently joining with the same id.");
                                }
                                else
                                {
                                    context.getLog( ClusterState.class ).info( "Denying entry to instance " + joiningId + " because that instance is already in the cluster.");
                                }
                                outgoing.offer( message.copyHeadersTo( respond( ClusterMessage.joinDenied, message,
                                        new ClusterMessage.ConfigurationResponseState( context.getConfiguration()
                                                .getRoles(), context.getConfiguration().getMembers(),
                                                new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( context.getLastDeliveredInstanceId() ),
                                                context.getConfiguration().getName() ) ) ) );
                            }
                            else
                            {
                                context.instanceIsJoining(joiningId, joiningUri );

                                outgoing.offer( message.copyHeadersTo( respond( ClusterMessage.configurationResponse, message,
                                        new ClusterMessage.ConfigurationResponseState( context.getConfiguration()
                                                .getRoles(), context.getConfiguration().getMembers(),
                                                new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( context.getLastDeliveredInstanceId() ),
                                                context.getConfiguration().getName() ) ) ) );
                            }
                            break;
                        }

                        case configurationChanged:
                        {
                            ClusterMessage.ConfigurationChangeState state = message.getPayload();
                            state.apply( context );
                            break;
                        }

                        case leave:
                        {
                            List<URI> nodeList = new ArrayList<URI>( context.getConfiguration().getMemberURIs() );
                            if ( nodeList.size() == 1 )
                            {
                                context.getLog( ClusterState.class ).info( "Shutting down cluster: " + context
                                        .getConfiguration().getName() );
                                context.left();

                                return start;

                            }
                            else
                            {
                                context.getLog( ClusterState.class ).info( "Leaving:" + nodeList );

                                ClusterMessage.ConfigurationChangeState newState = new ClusterMessage
                                        .ConfigurationChangeState();
                                newState.leave( context.getMyId() );

                                outgoing.offer( internal( AtomicBroadcastMessage.broadcast, newState ) );
                                context.setTimeout( "leave", timeout( ClusterMessage.leaveTimedout,
                                        message ) );

                                return leaving;
                            }
                        }
                    }

                    return this;
                }
            },

    leaving
            {
                @Override
                public State<?, ?> handle( ClusterContext context,
                                           Message<ClusterMessage> message,
                                           MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case configurationChanged:
                        {
                            ClusterMessage.ConfigurationChangeState state = message.getPayload();
                            if ( state.isLeaving( context.getMyId() ) )
                            {
                                context.cancelTimeout( "leave" );

                                context.left();

                                return start;
                            }
                            else
                            {
                                state.apply( context );
                                return leaving;
                            }
                        }

                        case leaveTimedout:
                        {
                            context.getLog( ClusterState.class ).warn( "Failed to leave. Cluster may consider this" +
                                    " instance still a member" );
                            context.left();
                            return start;
                        }
                    }

                    return this;
                }
            }
}
