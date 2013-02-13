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
package org.neo4j.cluster.protocol.heartbeat;

import static org.neo4j.cluster.com.message.Message.internal;
import static org.neo4j.cluster.com.message.Message.timeout;
import static org.neo4j.cluster.com.message.Message.to;

import java.net.URI;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.cluster.statemachine.State;

/**
 * State machine that implements the {@link Heartbeat} API
 */
public enum HeartbeatState
        implements State<HeartbeatContext, HeartbeatMessage>
{
    start
            {
                @Override
                public HeartbeatState handle( HeartbeatContext context,
                                              Message<HeartbeatMessage> message,
                                              MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case addHeartbeatListener:
                        {
                            context.addHeartbeatListener( (HeartbeatListener) message.getPayload() );
                            break;
                        }

                        case removeHeartbeatListener:
                        {
                            context.removeHeartbeatListener( (HeartbeatListener) message.getPayload() );
                            break;
                        }

                        case join:
                        {
                            // Setup heartbeat timeouts
                            context.startHeartbeatTimers( message );
                            return heartbeat;
                        }
                    }

                    return this;
                }
            },

    heartbeat
            {
                @Override
                public HeartbeatState handle( HeartbeatContext context,
                                              Message<HeartbeatMessage> message,
                                              MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case i_am_alive:
                        {
                            HeartbeatMessage.IAmAliveState state = (HeartbeatMessage.IAmAliveState) message
                                    .getPayload();

                            if ( context.alive( state.getServer() ) )
                            {
                                // Send suspicions messages to all non-failed servers
                                for ( URI aliveServer : context.getAlive() )
                                {
                                    if ( !aliveServer.equals( context.getClusterContext().getMe() ) )
                                    {
                                        outgoing.offer( Message.to( HeartbeatMessage.suspicions, aliveServer,
                                                new HeartbeatMessage.SuspicionsState( context.getSuspicionsFor( context
                                                        .getClusterContext()
                                                        .getMe() ) ) ) );
                                    }
                                }
                            }

                            context.getClusterContext().timeouts.cancelTimeout( HeartbeatMessage.i_am_alive + "-" +
                                    state.getServer() );
                            context.getClusterContext().timeouts.setTimeout( HeartbeatMessage.i_am_alive + "-" +
                                    state.getServer(), timeout( HeartbeatMessage.timed_out, message, state
                                    .getServer() ) );

                            // Check if this server knows something that we don't
                            if ( message.hasHeader( "last-learned" ) )
                            {
                                long lastLearned = Long.parseLong( message.getHeader( "last-learned" ) );
                                if ( lastLearned > context.getLearnerContext().getLastKnownLearnedInstanceInCluster() )
                                {
                                    outgoing.offer( internal( LearnerMessage.catchUp, lastLearned ) );
                                }
                            }

                            break;
                        }

                        case timed_out:
                        {
                            URI server = message.getPayload();
                            // Check if this node is no longer a part of the cluster
                            if ( context.getClusterContext().getConfiguration().getMembers().contains( server ) )
                            {
                                context.suspect( server );

                                context.getClusterContext().timeouts.setTimeout( HeartbeatMessage.i_am_alive + "-" +
                                        server, timeout( HeartbeatMessage.timed_out, message, server ) );

                                // Send suspicions messages to all non-failed servers
                                for ( URI aliveServer : context.getAlive() )
                                {
                                    if ( !aliveServer.equals( context.getClusterContext().getMe() ) )
                                    {
                                        outgoing.offer( Message.to( HeartbeatMessage.suspicions, aliveServer,
                                                new HeartbeatMessage.SuspicionsState( context.getSuspicionsFor( context
                                                        .getClusterContext()
                                                        .getMe() ) ) ) );
                                    }
                                }

                            }
                            else
                            {
                                // If no longer part of cluster, then don't bother
                                context.serverLeftCluster( server );
                            }
                            break;
                        }

                        case sendHeartbeat:
                        {
                            URI to = message.getPayload();

                            // Check if this node is no longer a part of the cluster
                            if ( context.getClusterContext().getConfiguration().getMembers().contains( to ) )
                            {
                                // Send heartbeat message to given server
                                outgoing.offer( to( HeartbeatMessage.i_am_alive, to,
                                        new HeartbeatMessage.IAmAliveState( context.getClusterContext().getMe() ) )
                                        .setHeader( "last-learned",
                                                context.getLearnerContext().getLastLearnedInstanceId() + "" ) );

                                // Set new timeout to send heartbeat to this host
                                context.getClusterContext().timeouts.setTimeout( HeartbeatMessage.sendHeartbeat + "-"
                                        + to, timeout( HeartbeatMessage.sendHeartbeat, message, to ) );
                            }
                            break;
                        }

                        case reset_send_heartbeat:
                        {
                            URI to = message.getPayload();
                            String timeoutName = HeartbeatMessage.sendHeartbeat + "-" + to;
                            context.getClusterContext().timeouts.cancelTimeout( timeoutName );
                            context.getClusterContext().timeouts.setTimeout( timeoutName, Message.timeout( HeartbeatMessage.sendHeartbeat, message, to ) );
                            break;
                        }

                        case suspicions:
                        {
                            HeartbeatMessage.SuspicionsState suspicions = message.getPayload();
                            URI from = new URI( message.getHeader( Message.FROM ) );
                            context.suspicions( from, suspicions.getSuspicions() );

                            break;
                        }

                        case leave:
                        {
                            return start;
                        }

                        case addHeartbeatListener:
                        {
                            context.addHeartbeatListener( (HeartbeatListener) message.getPayload() );
                            break;
                        }

                        case removeHeartbeatListener:
                        {
                            context.removeHeartbeatListener( (HeartbeatListener) message.getPayload() );
                            break;
                        }
                    }

                    return this;
                }
            }
}
