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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.statemachine.State;

/**
 * State machine for Paxos Acceptor
 */
public enum AcceptorState
        implements State<AcceptorContext, AcceptorMessage>
{
    start
            {
                @Override
                public AcceptorState handle( AcceptorContext context,
                                             Message<AcceptorMessage> message,
                                             MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case join:
                        {
                            return acceptor;
                        }
                    }

                    return this;
                }
            },

    acceptor
            {
                @Override
                public AcceptorState handle( AcceptorContext context,
                                             Message<AcceptorMessage> message,
                                             MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case prepare:
                        {
                            AcceptorMessage.PrepareState incomingState = message.getPayload();
                            InstanceId instanceId = new InstanceId( message );

                            // This creates the instance if not already present
                            AcceptorInstance localState = context.getAcceptorInstance( instanceId );

                            /*
                             * If the incoming messages has a ballot greater than the local one, send back a promise.
                             * This is always true for newly seen instances, the local state has ballot initialized
                             * to -1
                             */
                            if ( incomingState.getBallot() >= localState.getBallot() )
                            {
                                context.promise( localState, incomingState.getBallot() );

                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage.promise,
                                        message, new ProposerMessage.PromiseState( incomingState.getBallot(),
                                                localState.getValue() ) ), InstanceId.INSTANCE ) );
                            }
                            else
                            {
                                // Optimization - explicit reject
                                context.getLog( AcceptorState.class ).debug("Rejecting prepare from "
                                        + message.getHeader( Message.FROM ) + " for instance "
                                        + message.getHeader( InstanceId.INSTANCE ) + " and ballot "
                                        + incomingState.getBallot() + " (i had a prepare state ballot = "
                                        + localState.getBallot() + ")" );
                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage
                                        .rejectPrepare, message,
                                        new ProposerMessage.RejectPrepare( localState.getBallot() ) ),
                                        InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case accept:
                        {
                            // Task 4
                            AcceptorMessage.AcceptState acceptState = message.getPayload();
                            InstanceId instanceId = new InstanceId( message );
                            AcceptorInstance instance = context.getAcceptorInstance( instanceId );

                            if ( acceptState.getBallot() == instance.getBallot() )
                            {
                                context.accept( instance, acceptState.getValue() );
                                instance.accept( acceptState.getValue() );

                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage.accepted,
                                        message,
                                        new ProposerMessage.AcceptedState() ), InstanceId.INSTANCE ) );
                            }
                            else
                            {
                                context.getLog( AcceptorState.class ).debug( "Reject " + instanceId
                                        + " accept ballot:" + acceptState.getBallot() + " actual ballot:" +
                                        instance.getBallot() );
                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage
                                        .rejectAccept, message,
                                        new ProposerMessage.RejectAcceptState() ), InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case leave:
                        {
                            context.leave();
                            return start;
                        }
                    }

                    return this;
                }
            },
}
