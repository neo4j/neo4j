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
package org.neo4j.cluster.protocol.cluster;

import static org.neo4j.cluster.com.message.Message.internal;
import static org.neo4j.cluster.com.message.Message.respond;
import static org.neo4j.cluster.com.message.Message.timeout;
import static org.neo4j.cluster.com.message.Message.to;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.helpers.collection.Iterables;

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
                            context.getLogger( ClusterState.class ).info( "Creating cluster: " + name );
                            context.created( name );
                            return entered;
                        }

                        case join:
                        {
                            // Send configuration request to all instances
                            Object[] args = message.<Object[]>getPayload();
                            String name = ( String ) args[0];
                            URI[] clusterInstanceUris = ( URI[] ) args[1];
                            context.joining( name, Iterables.iterable( clusterInstanceUris ) );

                            for ( URI potentialClusterInstanceUri : clusterInstanceUris )
                            {
                                outgoing.offer( to( ClusterMessage.configurationRequest,
                                        potentialClusterInstanceUri ) );
                            }
                            context.timeouts.setTimeout( "discovery",
                                    timeout( ClusterMessage.configurationTimeout, message,
                                            new ClusterMessage.ConfigurationTimeoutState( 4 ) ) );
                            return acquiringConfiguration;
                        }
                    }
                    return this;
                }
            },

    acquiringConfiguration
            {
                @Override
                public State<?, ?> handle( ClusterContext context, Message<ClusterMessage> message,
                                           MessageHolder outgoing ) throws Throwable
                {
                    List<URI> discoveredInstances = context.getDiscoveredInstances();
                    switch ( message.getMessageType() )
                    {
                        case configurationResponse:
                        {
                            context.timeouts.cancelTimeout( "discovery" );

                            ClusterMessage.ConfigurationResponseState state = message.getPayload();

                            context.getLogger( ClusterState.class ).info( "Joining cluster " + state.getClusterName() );
                            if ( !context.getConfiguration().getName().equals( state.getClusterName() ) )
                            {
                                context.getLogger( ClusterState.class ).warn( "Joined cluster name is different than " +
                                        "the one configured. Expected " + context.getConfiguration().getName() +
                                        ", got " + state.getClusterName() + "." );
                            }

                            List<URI> memberList = new ArrayList<URI>( state.getMembers() );
                            context.learnerContext.setLastDeliveredInstanceId( state.getLatestReceivedInstanceId
                                    ().getId() );
                            context.learnerContext.learnedInstanceId( state.getLatestReceivedInstanceId().getId() );
                            context.proposerContext.nextInstanceId = state.getLatestReceivedInstanceId().getId()
                                    + 1;

                            context.acquiredConfiguration( memberList, state.getRoles() );

                            if ( !memberList.contains( context.me ) )
                            {
                                context.getLogger( ClusterState.class ).info( String.format( "%s joining:%s, " +
                                        "last delivered:%d", context.me.toString(),
                                        context.getConfiguration().toString(),
                                        state.getLatestReceivedInstanceId().getId() ) );

                                ClusterMessage.ConfigurationChangeState newState = new ClusterMessage
                                        .ConfigurationChangeState();
                                newState.join( context.me );

                                // Let the coordinator propose this if possible
                                URI coordinator = state.getRoles().get( ClusterConfiguration.COORDINATOR );
                                if ( coordinator != null )
                                {
                                    outgoing.offer( to( ProposerMessage.propose, coordinator, newState ) );
                                }
                                else
                                {
                                    outgoing.offer( to( ProposerMessage.propose, new URI( message.getHeader(
                                            Message.FROM ) ), newState ) );
                                }

                                context.getLogger( ClusterState.class ).info( "Setup join timeout for " + message
                                        .getHeader( Message.CONVERSATION_ID ) );
                                context.timeouts.setTimeout( "join", timeout( ClusterMessage.joiningTimeout, message,
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
                            ClusterMessage.ConfigurationTimeoutState state = message.getPayload();
                            if ( state.getRemainingPings() > 0 )
                            {
                                // Send out requests again
                                for ( URI potentialClusterInstanceUri : context.getJoiningInstances() )
                                {
                                    outgoing.offer( to( ClusterMessage.configurationRequest,
                                            potentialClusterInstanceUri ) );
                                }
                                context.timeouts.setTimeout( "join",
                                        timeout( ClusterMessage.configurationTimeout, message,
                                                new ClusterMessage.ConfigurationTimeoutState(
                                                        state.getRemainingPings()-1 ) ) );


                            }
                            else
                            {
                                // No responses
                                // Check if we picked up any other instances' requests during this phase
                                if ( !discoveredInstances.isEmpty() )
                                {
                                    Collections.sort( discoveredInstances );
                                    /*
                                     * The assumption here is that the lowest in the list of discovered instances
                                     * will create the cluster. Keep in mind that this is run on all instances so
                                     * everyone will pick the same one.
                                     * If the one picked up is configured to not init a cluster then the timeout
                                     * set in else{} will take care of that.
                                     */
                                    if ( discoveredInstances.get( 0 ).compareTo( context.getMe() ) > 0 )
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
                                                    potentialClusterInstanceUri ) );
                                        }
                                        context.timeouts.setTimeout( "discovery",
                                                timeout( ClusterMessage.configurationTimeout, message,
                                                        new ClusterMessage.ConfigurationTimeoutState( 4 ) ) );
                                    }
                                }
                                else
                                {
                                    // Join failed
                                    outgoing.offer( internal( ClusterMessage.joinFailure,
                                            new TimeoutException(
                                                    "Join failed, timeout waiting for configuration" ) ) );
                                    return start;
                                }
                            }

                            return this;
                        }

                        case configurationRequest:
                        {
                            // We're listening for existing clusters, but if all instances start up at the same time
                            // and look for each other, this allows us to pick that up
                            URI joiningInstanceUri = new URI( message.getHeader( Message.FROM ) );
                            if ( !discoveredInstances.contains( joiningInstanceUri ) )
                            {
                                discoveredInstances.add( joiningInstanceUri );
                            }
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

                            if ( context.getMe().equals( state.getJoin() ) )
                            {
                                context.timeouts.cancelTimeout( "join" );

                                context.joined();
                                outgoing.offer( internal( ClusterMessage.joinResponse, context.getConfiguration() ) );
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
                            context.getLogger( ClusterState.class ).info( "Join timeout for " + message.getHeader(
                                    Message.CONVERSATION_ID ) );

                            // Go back to requesting configurations from potential members
                            for ( URI potentialClusterInstanceUri : context.getJoiningInstances() )
                            {
                                outgoing.offer( to( ClusterMessage.configurationRequest,
                                        potentialClusterInstanceUri ) );
                            }
                            context.timeouts.setTimeout( "discovery",
                                    timeout( ClusterMessage.configurationTimeout, message,
                                            new ClusterMessage.ConfigurationTimeoutState( 4 ) ) );

                            return acquiringConfiguration;
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
                            outgoing.offer( respond( ClusterMessage.configurationResponse, message,
                                    new ClusterMessage.ConfigurationResponseState( context.getConfiguration()
                                            .getRoles(),
                                            context.getConfiguration().getMembers(),
                                            new InstanceId( context.learnerContext.getLastDeliveredInstanceId() ),
                                            context.getConfiguration().getName() ) ) );
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
                            List<URI> nodeList = new ArrayList<URI>( context.getConfiguration().getMembers() );
                            if ( nodeList.size() == 1 )
                            {
                                context.getLogger( ClusterState.class ).info( "Shutting down cluster: " + context
                                        .getConfiguration().getName() );
                                context.left();

                                return start;

                            }
                            else
                            {
                                context.getLogger( ClusterState.class ).info( "Leaving:" + nodeList );

                                ClusterMessage.ConfigurationChangeState newState = new ClusterMessage
                                        .ConfigurationChangeState();
                                newState.leave( context.me );

                                outgoing.offer( internal( AtomicBroadcastMessage.broadcast, newState ) );
                                context.timeouts.setTimeout( "leave", timeout( ClusterMessage.leaveTimedout,
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
                            if ( state.isLeaving( context.getMe() ) )
                            {
                                context.timeouts.cancelTimeout( "leave" );

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
                            context.getLogger( ClusterState.class ).warn( "Failed to leave. Cluster may consider this" +
                                    " instance still a " +
                                    "member" );
                            context.left();
                            return start;
                        }
                    }

                    return this;
                }
            }
}
