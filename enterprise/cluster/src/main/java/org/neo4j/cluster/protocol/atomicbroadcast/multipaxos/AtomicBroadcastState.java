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

package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import static org.neo4j.cluster.com.message.Message.internal;
import static org.neo4j.cluster.com.message.Message.timeout;
import static org.neo4j.cluster.com.message.Message.to;

import java.net.URI;
import java.util.concurrent.TimeoutException;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.cluster.timeout.Timeouts;

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
                        throws Throwable
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
                            defaultHandling( context, message, outgoing );
                        }
                    }

                    return this;
                }
            },

    joining
            {
                @Override
                public State<?, ?> handle( AtomicBroadcastContext context,
                                           Message<AtomicBroadcastMessage> message,
                                           MessageHolder outgoing
                )
                        throws Throwable
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
                                outgoing.offer( internal( ClusterMessage.configurationChanged,
                                        message.getPayload() ) );
                            }

                            break;
                        }

                        case entered:
                        {
                            return broadcasting;
                        }

                        default:
                        {
                            defaultHandling( context, message, outgoing );
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
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case broadcast:
                        {
                            URI coordinator = context.getCoordinator();
                            if ( coordinator != null )
                            {
                                outgoing.offer( to( ProposerMessage.propose, coordinator, message.getPayload() ) );
                                Timeouts timeouts = context.getClusterContext().timeouts;
                                timeouts.setTimeout( "broadcast-" + message.getHeader( Message.CONVERSATION_ID ),
                                        timeout( AtomicBroadcastMessage.broadcastTimeout, message,
                                                message.getPayload() ) );
                            }
                            else
                            {
                                outgoing.offer( message.copyHeadersTo( internal( ProposerMessage.propose,
                                        message.getPayload() ), Message.CONVERSATION_ID ) );
                            }
                            break;
                        }

                        case broadcastResponse:
                        {
                            context.getClusterContext().timeouts.cancelTimeout( "broadcast-" + message.getHeader(
                                    Message.CONVERSATION_ID ) );

                            // TODO FILTER MESSAGES

                            if ( message.getPayload() instanceof ClusterMessage.ConfigurationChangeState )
                            {
                                outgoing.offer( internal( ClusterMessage.configurationChanged,
                                        message.getPayload() ) );
                            }
                            else
                            {
                                context.receive( message.<Payload>getPayload() );
                            }

                            break;
                        }

                        case broadcastTimeout:
                        {
                            outgoing.offer( internal( AtomicBroadcastMessage.broadcast, message.getPayload() ) );
                            break;
                        }

                        case leave:
                        {
                            return start;
                        }

                        default:
                        {
                            defaultHandling( context, message, outgoing );
                        }
                    }

                    return this;
                }
            };

    private static void defaultHandling( AtomicBroadcastContext context, Message<AtomicBroadcastMessage> message,
                                         MessageHolder outgoing )
    {
        switch ( message.getMessageType() )
        {
            case addAtomicBroadcastListener:
            {
                context.addAtomicBroadcastListener( (AtomicBroadcastListener) message.getPayload() );
                break;
            }

            case removeAtomicBroadcastListener:
            {
                context.removeAtomicBroadcastListener( (AtomicBroadcastListener) message.getPayload() );
                break;
            }
        }
    }
}
