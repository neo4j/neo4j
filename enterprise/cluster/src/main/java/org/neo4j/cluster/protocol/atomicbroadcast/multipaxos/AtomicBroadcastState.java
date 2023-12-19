/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.concurrent.TimeoutException;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.statemachine.State;

import static org.neo4j.cluster.com.message.Message.internal;
import static org.neo4j.cluster.com.message.Message.timeout;
import static org.neo4j.cluster.com.message.Message.to;

/**
 * State Machine for implementation of Atomic Broadcast client interface
 */
public enum AtomicBroadcastState
        implements State<AtomicBroadcastContext, AtomicBroadcastMessage>
{
    start
            {
                @Override
                public AtomicBroadcastState handle( AtomicBroadcastContext context,
                                                    Message<AtomicBroadcastMessage> message,
                                                    MessageHolder outgoing
                )
                {

                    switch ( message.getMessageType() )
                    {
                        case entered:
                        {
                            return broadcasting;
                        }

                        case join:
                        {
                            return joining;
                        }

                        default:
                        {
                            defaultHandling( context, message );
                        }
                    }

                    return this;
                }
            },

    joining
            {
                @Override
                public AtomicBroadcastState handle( AtomicBroadcastContext context,
                                           Message<AtomicBroadcastMessage> message,
                                           MessageHolder outgoing
                )
                {
                    switch ( message.getMessageType() )
                    {
                        case failed:
                        {
                            // Joining failed
                            outgoing.offer( internal( ClusterMessage.joinFailure,
                                    new TimeoutException( "Could not join cluster" ) ) );

                            return start;
                        }

                        case broadcastResponse:
                        {
                            if ( message.getPayload() instanceof ClusterMessage.ConfigurationChangeState )
                            {
                                outgoing.offer( message.copyHeadersTo( internal( ClusterMessage.configurationChanged,
                                        message.getPayload() ) ) );
                            }

                            break;
                        }

                        case entered:
                        {
                            return broadcasting;
                        }

                        default:
                        {
                            defaultHandling( context, message );
                        }
                    }

                    return this;
                }
            },

    broadcasting
            {
                @Override
                public AtomicBroadcastState handle( AtomicBroadcastContext context,
                                                    Message<AtomicBroadcastMessage> message,
                                                    MessageHolder outgoing
                )
                {
                    switch ( message.getMessageType() )
                    {
                        case broadcast:
                        case failed:
                        {
                            if ( context.hasQuorum() )
                            {
                                org.neo4j.cluster.InstanceId coordinator = context.getCoordinator();
                                if ( coordinator != null )
                                {
                                    URI coordinatorUri = context.getUriForId( coordinator );
                                    outgoing.offer( message.copyHeadersTo(
                                            to( ProposerMessage.propose, coordinatorUri, message.getPayload() ) ) );
                                    context.setTimeout( "broadcast-" + message.getHeader( Message.HEADER_CONVERSATION_ID ),
                                            timeout( AtomicBroadcastMessage.broadcastTimeout, message,
                                                    message.getPayload() ) );
                                }
                                else
                                {
                                    outgoing.offer( message.copyHeadersTo( internal( ProposerMessage.propose,
                                            message.getPayload() ), Message.HEADER_CONVERSATION_ID, org.neo4j.cluster.protocol
                                            .atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                }
                            }
                            else
                            {
                                context.getLog( AtomicBroadcastState.class )
                                       .warn( "No quorum and therefor dropping broadcast msg: " + message.getPayload() );
                            }
                            break;
                        }

                        case broadcastResponse:
                        {
                            context.cancelTimeout( "broadcast-" + message.getHeader( Message.HEADER_CONVERSATION_ID ) );

                            // TODO FILTER MESSAGES

                            if ( message.getPayload() instanceof ClusterMessage.ConfigurationChangeState )
                            {
                                outgoing.offer( message.copyHeadersTo( internal( ClusterMessage.configurationChanged,
                                        message.getPayload() ) ) );
                                ClusterMessage.ConfigurationChangeState change = message.getPayload();
                                if ( change.getJoinUri() != null )
                                {
                                    outgoing.offer( message.copyHeadersTo(
                                            Message.internal( HeartbeatMessage.i_am_alive,
                                                    new HeartbeatMessage.IAmAliveState( change.getJoin() ) ),
                                            Message.HEADER_FROM ) );
                                }
                            }
                            else
                            {
                                context.receive( message.getPayload() );
                            }

                            break;
                        }

                        case broadcastTimeout:
                        {
                            /*
                             * There is never the need to rebroadcast on broadcast timeout. The propose message always
                             * circulates on the wire until it is accepted - it comes back here when it fails just to
                             * check if the coordinator changed (look at "failed/broadcast" handling above).
                             */
//                            outgoing.offer( internal( AtomicBroadcastMessage.broadcast, message.getPayload() ) );
                            break;
                        }

                        case leave:
                        {
                            return start;
                        }

                        default:
                        {
                            defaultHandling( context, message );
                        }
                    }

                    return this;
                }
            };

    private static void defaultHandling( AtomicBroadcastContext context, Message<AtomicBroadcastMessage> message )
    {
        switch ( message.getMessageType() )
        {
            case addAtomicBroadcastListener:
            {
                context.addAtomicBroadcastListener( message.getPayload() );
                break;
            }

            case removeAtomicBroadcastListener:
            {
                context.removeAtomicBroadcastListener( message.getPayload() );
                break;
            }

            default:
                break;
        }
    }
}
