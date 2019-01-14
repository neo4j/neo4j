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
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.net.URISyntaxException;
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

import static java.lang.String.format;
import static org.neo4j.cluster.com.message.Message.DISCOVERED;
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
public enum ClusterState implements State<ClusterContext, ClusterMessage>
{
    start
            {
                @Override
                public ClusterState handle( ClusterContext context, Message<ClusterMessage> message,
                                           MessageHolder outgoing )
                {
                    switch ( message.getMessageType() )
                    {
                        case addClusterListener:
                        {
                            context.addClusterListener( message.getPayload() );

                            break;
                        }

                        case removeClusterListener:
                        {
                            context.removeClusterListener( message.getPayload() );

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
                            Object[] args = message.getPayload();
                            String name = (String) args[0];
                            URI[] clusterInstanceUris = (URI[]) args[1];
                            context.joining( name, Iterables.iterable( clusterInstanceUris ) );
                            context.getLog( getClass() )
                                    .info( "Trying to join with DISCOVERY header " + context.generateDiscoveryHeader() );

                            for ( URI potentialClusterInstanceUri : clusterInstanceUris )
                            {
                                /*
                                 * The DISCOVERY header is empty, since we haven't processed configurationRequests
                                 * at all yet. However, we still send it out for consistency.
                                 */
                                outgoing.offer( to( ClusterMessage.configurationRequest,
                                        potentialClusterInstanceUri,
                                        new ClusterMessage.ConfigurationRequestState( context.getMyId(), context.boundAt() ) )
                                        .setHeader( DISCOVERED, context.generateDiscoveryHeader() ) );
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

                        default:
                            break;
                    }
                    return this;
                }
            },

    discovery
            {
                @Override
                public ClusterState handle( ClusterContext context, Message<ClusterMessage> message,
                                           MessageHolder outgoing ) throws URISyntaxException
                {
                    List<ClusterMessage.ConfigurationRequestState> discoveredInstances = context.getDiscoveredInstances();
                    context.getLog( getClass() ).info( format( "Discovered instances are %s", discoveredInstances ) );
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

                            HashMap<InstanceId, URI> memberList = new HashMap<>( state.getMembers() );
                            context.discoveredLastReceivedInstanceId( state.getLatestReceivedInstanceId().getId() );

                            context.acquiredConfiguration( memberList, state.getRoles(), state.getFailedMembers() );

                            if ( !memberList.containsKey( context.getMyId() ) ||
                                    !memberList.get( context.getMyId() ).equals( context.boundAt() ) )
                            {
                                context.getLog( ClusterState.class ).info( format( "%s joining:%s, last delivered:%d",
                                        context.getMyId().toString(), context.getConfiguration().toString(),
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
                                            Message.HEADER_FROM ) ), newState ) );
                                }

                                context.getLog( ClusterState.class ).debug( "Setup join timeout for " + message
                                        .getHeader( Message.HEADER_CONVERSATION_ID ) );
                                context.setTimeout( "join", timeout( ClusterMessage.joiningTimeout, message,
                                        new URI( message.getHeader( Message.HEADER_FROM ) ) ) );

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
                                context.getLog( getClass() ).info( format( "Trying to join with DISCOVERY header %s",
                                        context.generateDiscoveryHeader() ) );
                                // Send out requests again
                                for ( URI potentialClusterInstanceUri : context.getJoiningInstances() )
                                {
                                    outgoing.offer( to( ClusterMessage.configurationRequest,
                                            potentialClusterInstanceUri,
                                            new ClusterMessage.ConfigurationRequestState(
                                                    context.getMyId(), context.boundAt() ) )
                                            .setHeader( DISCOVERED, context.generateDiscoveryHeader() ) );
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
                                            new ClusterMessage.ConfigurationRequestState( context.getMyId(), context.boundAt() );
                                    // No one to join with
                                    boolean imAlone =
                                            count(context.getJoiningInstances()) == 1
                                            && discoveredInstances.contains( ourRequestState )
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
                                        context.getLog( getClass() ).info( format(
                                                "Trying to join with DISCOVERY header %s",
                                                context.generateDiscoveryHeader() ) );
                                        // Someone else is supposed to create the cluster - restart the join discovery
                                        for ( URI potentialClusterInstanceUri : context.getJoiningInstances() )
                                        {
                                            outgoing.offer( to( ClusterMessage.configurationRequest,
                                                    potentialClusterInstanceUri,
                                                    new ClusterMessage.ConfigurationRequestState( context.getMyId(),
                                                            context.boundAt() ) )
                                                    .setHeader( DISCOVERED, context.generateDiscoveryHeader() ));
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
                            configurationRequested = new ClusterMessage.ConfigurationRequestState(
                                    configurationRequested.getJoiningId(),
                                    URI.create( message.getHeader( Message.HEADER_FROM ) ) );
                            // Make a note that this instance contacted us.
                            context.addContactingInstance( configurationRequested, message.getHeader( DISCOVERED, "" ) );
                            context.getLog( getClass() ).info( format( "Received configuration request %s and " +
                                    "the header was %s", configurationRequested, message.getHeader( DISCOVERED, "" ) ) );

                            if ( !discoveredInstances.contains( configurationRequested ) )
                            {
                                for ( ClusterMessage.ConfigurationRequestState discoveredInstance :
                                        discoveredInstances )
                                {
                                    if ( discoveredInstance.getJoiningId().equals( configurationRequested.getJoiningId() ) )
                                    {
                                        // we are done
                                        outgoing.offer( internal( ClusterMessage.joinFailure,
                                                new IllegalStateException( format(
                                                        "Failed to join cluster because I saw two instances with the " +
                                                                "same ServerId. One is %s. The other is %s",
                                                        discoveredInstance, configurationRequested ) ) ) );
                                        return start;
                                    }
                                }
                                if ( context.shouldFilterContactingInstances() )
                                {
                                    if ( context.haveWeContactedInstance( configurationRequested ) )
                                    {
                                        context.getLog( getClass() ).info( format( "%s had header %s which " +
                                                "contains us. This means we've contacted them and they are in our " +
                                                "initial hosts.", configurationRequested, message.getHeader( DISCOVERED, "" ) ) );

                                        discoveredInstances.add( configurationRequested );
                                    }
                                    else
                                    {
                                        context.getLog( getClass() ).warn(
                                                format( "joining instance %s was not in %s, i will not consider it " +
                                                                "for " +
                                                                "purposes of cluster creation",
                                                        configurationRequested.getJoiningUri(),
                                                        context.getJoiningInstances() ) );
                                    }
                                }
                                else
                                {
                                    discoveredInstances.add( configurationRequested );
                                }
                            }
                            break;
                        }

                        case joinDenied:
                        {
                            context.joinDenied( message.getPayload() );
                            return this;
                        }

                        default:
                            break;
                    }

                    return this;
                }
            },

    joining
            {
                @Override
                public ClusterState handle( ClusterContext context,
                                           Message<ClusterMessage> message,
                                           MessageHolder outgoing
                )
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
                                outgoing.offer( message.copyHeadersTo(
                                        internal( ClusterMessage.joinResponse, context.getConfiguration() ) ) );
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
                                    Message.HEADER_CONVERSATION_ID ) );

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
                                        new ClusterMessage.ConfigurationRequestState( context.getMyId(), context.boundAt() ) )
                                        .setHeader( DISCOVERED, context.generateDiscoveryHeader() ));
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

                        default:
                            break;
                    }

                    return this;
                }
            },

    entered
            {
                @Override
                public ClusterState handle( ClusterContext context, Message<ClusterMessage> message,
                                           MessageHolder outgoing )
                {
                    switch ( message.getMessageType() )
                    {
                        case addClusterListener:
                        {
                            context.addClusterListener( message.getPayload() );

                            break;
                        }

                        case removeClusterListener:
                        {
                            context.removeClusterListener( message.getPayload() );

                            break;
                        }

                        case configurationRequest:
                        {
                            ClusterMessage.ConfigurationRequestState request = message.getPayload();
                            request = new ClusterMessage.ConfigurationRequestState( request.getJoiningId(),
                                    URI.create( message.getHeader( Message.HEADER_FROM ) ) );

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
                                if ( otherInstanceJoiningWithSameId )
                                {
                                    context.getLog( ClusterState.class ).info( format( "Denying entry to instance %s" +
                                            " because another instance is currently joining with the same id.",
                                            joiningId ) );
                                }
                                else
                                {
                                    context.getLog( ClusterState.class ).info( format( "Denying entry to " +
                                            "instance %s because that instance is already in the cluster.", joiningId ) );
                                }
                                outgoing.offer( message.copyHeadersTo( respond( ClusterMessage.joinDenied, message,
                                        new ClusterMessage.ConfigurationResponseState( context.getConfiguration()
                                                .getRoles(), context.getConfiguration().getMembers(),
                                                new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId(
                                                        context.getLastDeliveredInstanceId() ),
                                                context.getFailedInstances(),
                                                context.getConfiguration().getName() ) ) ) );
                            }
                            else
                            {
                                context.instanceIsJoining(joiningId, joiningUri );

                                outgoing.offer( message.copyHeadersTo( respond( ClusterMessage.configurationResponse, message,
                                        new ClusterMessage.ConfigurationResponseState( context.getConfiguration()
                                                .getRoles(), context.getConfiguration().getMembers(),
                                                new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId(
                                                        context.getLastDeliveredInstanceId() ),
                                                context.getFailedInstances(),
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
                            List<URI> nodeList = new ArrayList<>( context.getConfiguration().getMemberURIs() );
                            if ( nodeList.size() == 1 )
                            {
                                context.getLog( ClusterState.class ).info( format( "Shutting down cluster: %s",
                                        context.getConfiguration().getName() ) );
                                context.left();

                                return start;

                            }
                            else
                            {
                                context.getLog( ClusterState.class ).info( format( "Leaving:%s", nodeList ) );

                                ClusterMessage.ConfigurationChangeState newState = new ClusterMessage
                                        .ConfigurationChangeState();
                                newState.leave( context.getMyId() );

                                outgoing.offer( internal( AtomicBroadcastMessage.broadcast, newState ) );
                                context.setTimeout( "leave", timeout( ClusterMessage.leaveTimedout,
                                        message ) );

                                return leaving;
                            }
                        }

                        default:
                            break;
                    }

                    return this;
                }
            },

    leaving
            {
                @Override
                public ClusterState handle( ClusterContext context,
                                           Message<ClusterMessage> message,
                                           MessageHolder outgoing
                )
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

                        default:
                            break;
                    }

                    return this;
                }
            }
}
