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
package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.cluster.statemachine.State;

import static java.lang.String.format;
import static org.neo4j.cluster.com.message.Message.internal;
import static org.neo4j.cluster.com.message.Message.timeout;
import static org.neo4j.cluster.com.message.Message.to;

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
                            for ( InstanceId instanceId : context.getOtherInstances() )
                            {
                                // Setup heartbeat timeouts for the other instance
                                context.setTimeout(
                                        HeartbeatMessage.i_am_alive + "-" + instanceId,
                                        timeout( HeartbeatMessage.timed_out, message, instanceId ) );

                                // Send first heartbeat immediately
                                outgoing.offer( timeout( HeartbeatMessage.sendHeartbeat, message, instanceId ) );
                            }

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
                            HeartbeatMessage.IAmAliveState state = message.getPayload();

                            if ( context.isMe( state.getServer() ) )
                            {
                                break;
                            }


                            if ( state.getServer() == null )
                            {
                                break;
                            }

                            if ( context.alive( state.getServer() ) )
                            {
                                // Send suspicions messages to all non-failed servers
                                for ( InstanceId aliveServer : context.getAlive() )
                                {
                                    if ( !aliveServer.equals( context.getMyId() ) )
                                    {
                                        URI aliveServerUri =
                                                context.getUriForId( aliveServer );
                                        outgoing.offer( Message.to( HeartbeatMessage.suspicions, aliveServerUri,
                                                new HeartbeatMessage.SuspicionsState( context.getSuspicionsFor(
                                                        context.getMyId() ) ) ) );
                                    }
                                }
                            }

                            resetTimeout( context, message, state );

                            // Check if this server knows something that we don't
                            if ( message.hasHeader( "last-learned" ) )
                            {
                                long lastLearned = Long.parseLong( message.getHeader( "last-learned" ) );
                                if ( lastLearned > context.getLastKnownLearnedInstanceInCluster() )
                                {
                                    /*
                                     * Need to pass the INSTANCE_ID header to catchUp state,
                                     * as the instance in catchUp state should be aware of at least one
                                     * alive member of the cluster. FROM used to be abused for this reason
                                     * previously, so we leave it here for legacy reasons - should really have
                                     * no use within the current codebase but mixed version clusters may
                                     * make use of it.
                                     */
                                    Message<LearnerMessage> catchUpMessage = message.copyHeadersTo(
                                            internal( LearnerMessage.catchUp, lastLearned ),
                                            Message.FROM, Message.INSTANCE_ID );
                                    outgoing.offer( catchUpMessage );
                                }
                            }

                            break;
                        }

                        case timed_out:
                        {
                            InstanceId server = message.getPayload();
                            context.getLog( HeartbeatState.class )
                                    .debug( "Received timed out for server " + server );
                            // Check if this node is no longer a part of the cluster
                            if ( context.getMembers().containsKey( server ) )
                            {
                                context.suspect( server );

                                context.setTimeout( HeartbeatMessage.i_am_alive + "-" +
                                        server, timeout( HeartbeatMessage.timed_out, message, server ) );

                                // Send suspicions messages to all non-failed servers
                                for ( InstanceId aliveServer : context.getAlive() )
                                {
                                    if ( !aliveServer.equals( context.getMyId() ) )
                                    {
                                        URI sendTo = context.getUriForId( aliveServer );
                                        outgoing.offer( Message.to( HeartbeatMessage.suspicions, sendTo,
                                                new HeartbeatMessage.SuspicionsState( context.getSuspicionsFor(
                                                        context.getMyId() ) ) ) );
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
                            InstanceId to = message.getPayload();

                            if ( !context.isMe( to ) )
                            {
                                // Check if this node is no longer a part of the cluster
                                if ( context.getMembers().containsKey( to ) )
                                {
                                    URI toSendTo = context.getUriForId( to );
                                    // Send heartbeat message to given server
                                    outgoing.offer( to( HeartbeatMessage.i_am_alive, toSendTo,
                                            new HeartbeatMessage.IAmAliveState(
                                                    context.getMyId() ) )
                                            .setHeader( "last-learned",
                                                    context.getLastLearnedInstanceId() + "" ) );

                                    // Set new timeout to send heartbeat to this host
                                    context.setTimeout(
                                            HeartbeatMessage.sendHeartbeat + "-" + to,
                                            timeout( HeartbeatMessage.sendHeartbeat, message, to ) );
                                }
                            }
                            break;
                        }

                        case reset_send_heartbeat:
                        {
                            InstanceId to = message.getPayload();

                            if ( !context.isMe( to ) )
                            {
                                String timeoutName = HeartbeatMessage.sendHeartbeat + "-" + to;
                                context.cancelTimeout( timeoutName );
                                context.setTimeout( timeoutName, Message.timeout(
                                        HeartbeatMessage.sendHeartbeat, message, to ) );
                            }
                            break;
                        }

                        case suspicions:
                        {
                            HeartbeatMessage.SuspicionsState suspicions = message.getPayload();

                            InstanceId fromId = new InstanceId(
                                    Integer.parseInt( message.getHeader( Message.INSTANCE_ID ) ) );

                            context.getLog( HeartbeatState.class )
                                    .debug( format( "Received suspicions as %s from %s", suspicions, fromId ) );

                            /*
                             * Remove ourselves from the suspicions received - we just received a message,
                             * it's not normal to be considered failed. Whatever it was, it was transient and now it has
                             * passed.
                             */
                            suspicions.getSuspicions().remove( context.getMyId() );
                            context.suspicions( fromId, suspicions.getSuspicions() );

                            break;
                        }

                        case leave:
                        {
                            context.getLog( HeartbeatState.class ).debug( "Received leave" );
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

                private void resetTimeout( HeartbeatContext context, Message<HeartbeatMessage> message,
                                           HeartbeatMessage.IAmAliveState state )
                {
                    String key = HeartbeatMessage.i_am_alive + "-" + state.getServer();
                    Message<? extends MessageType> oldTimeout = context.cancelTimeout( key );
                    if ( oldTimeout != null && oldTimeout.hasHeader( Message.TIMEOUT_COUNT ) )
                    {
                        int timeoutCount = Integer.parseInt( oldTimeout.getHeader( Message.TIMEOUT_COUNT ) );
                        if ( timeoutCount > 0 )
                        {
                            long timeout = context.getTimeoutFor( oldTimeout );
                            context.getLog( HeartbeatState.class ).debug(
                                    "Received " + state + " after missing " + timeoutCount +
                                            " (" + timeout * timeoutCount + "ms)" );
                        }
                    }
                    context.setTimeout( key, timeout( HeartbeatMessage.timed_out, message, state.getServer() ) );
                }
            }
}
