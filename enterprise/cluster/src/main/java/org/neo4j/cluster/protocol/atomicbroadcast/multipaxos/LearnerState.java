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

import java.net.URI;
import java.net.URISyntaxException;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.logging.Log;

/**
 * State machine for Paxos Learner
 */
public enum LearnerState
        implements State<LearnerContext, LearnerMessage>
{
    start
            {
                @Override
                public LearnerState handle( LearnerContext context,
                                            Message<LearnerMessage> message,
                                            MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case join:
                        {
                            return learner;
                        }
                    }

                    return this;
                }
            },

    learner
            {
                @Override
                public LearnerState handle( LearnerContext context,
                                            Message<LearnerMessage> message,
                                            MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case learn:
                        {
                            LearnerMessage.LearnState learnState = message.getPayload();
                            InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstance( instanceId );

                            Log log = context.getLog( getClass() );

                            // Skip if we already know about this
                            if ( instanceId.getId() <= context.getLastDeliveredInstanceId() )
                            {
                                break;
                            }

                            context.learnedInstanceId( instanceId.getId() );

                            instance.closed( learnState.getValue(), message.getHeader( Message.CONVERSATION_ID ) );

                            /*
                             * The conditional below is simply so that no expensive deserialization will happen if we
                             * are not to print anything anyway if debug is not enabled.
                             */
                            if ( log.isDebugEnabled() )
                            {
                                String description;
                                if ( instance.value_2 instanceof Payload )
                                {
                                    AtomicBroadcastSerializer atomicBroadcastSerializer = context.newSerializer();

                                    description = atomicBroadcastSerializer.receive( (Payload) instance.value_2 ).toString();
                                }
                                else
                                {
                                    description = instance.value_2.toString();
                                }
                                log.debug(
                                        "Learned and closed instance "+instance.id +
                                                " from conversation " +
                                                instance.conversationIdHeader +
                                                " and the content was " +
                                                description );
                            }
                            // If this is the next instance to be learned, then do so and check if we have anything
                            // pending to be learned
                            if ( instanceId.getId() == context.getLastDeliveredInstanceId() + 1 )
                            {
                                instance.delivered();
                                outgoing.offer( Message.internal( AtomicBroadcastMessage.broadcastResponse,
                                        learnState.getValue() )
                                        .setHeader( InstanceId.INSTANCE, instance.id.toString() )
                                        .setHeader( Message.CONVERSATION_ID, instance.conversationIdHeader ));
                                context.setLastDeliveredInstanceId( instanceId.getId() );

                                long checkInstanceId = instanceId.getId() + 1;
                                while ( (instance = context.getPaxosInstance( new InstanceId(
                                        checkInstanceId ) )).isState( PaxosInstance.State.closed ) )
                                {
                                    instance.delivered();
                                    context.setLastDeliveredInstanceId( checkInstanceId );
                                    Message<AtomicBroadcastMessage> learnMessage = Message.internal(
                                            AtomicBroadcastMessage.broadcastResponse, instance.value_2 )
                                            .setHeader( InstanceId.INSTANCE, instance.id.toString() )
                                            .setHeader( Message.CONVERSATION_ID, instance.conversationIdHeader );
                                    outgoing.offer( learnMessage );

                                    checkInstanceId++;
                                }

                                if ( checkInstanceId == context.getLastKnownLearnedInstanceInCluster() + 1 )
                                {
                                    // No hole - all is ok
                                    // Cancel potential timeout, if one is active
                                    context.cancelTimeout( "learn" );
                                }
                                else
                                {
                                    // Found hole - we're waiting for this to be filled, i.e. timeout already set
                                    context.getLog( LearnerState.class ).debug( "*** HOLE! WAITING " +
                                            "FOR " + (context.getLastDeliveredInstanceId() + 1) );
                                }
                            }
                            else
                            {
                                // Found hole - we're waiting for this to be filled, i.e. timeout already set
                                context.getLog( LearnerState.class ).debug( "*** GOT " + instanceId
                                        + ", WAITING FOR " + (context.getLastDeliveredInstanceId() + 1) );

                                context.setTimeout( "learn", Message.timeout( LearnerMessage.learnTimedout,
                                        message ) );
                            }
                            break;
                        }

                        case learnTimedout:
                        {
                            // Timed out waiting for learned values - send explicit request to everyone that is not failed
                            if ( !context.hasDeliveredAllKnownInstances() )
                            {
                                for ( long instanceId = context.getLastDeliveredInstanceId() + 1;
                                      instanceId < context.getLastKnownLearnedInstanceInCluster();
                                      instanceId++ )
                                {
                                    InstanceId id = new InstanceId( instanceId );
                                    PaxosInstance instance = context.getPaxosInstance( id );
                                    if ( !instance.isState( PaxosInstance.State.closed ) && !instance.isState(
                                            PaxosInstance.State.delivered ) )
                                    {
                                        for ( org.neo4j.cluster.InstanceId node : context.getAlive() )
                                        {
                                            URI nodeUri = context.getUriForId( node );
                                            if ( !node.equals( context.getMyId() ) )
                                            {
                                                outgoing.offer( Message.to( LearnerMessage.learnRequest, nodeUri,
                                                        new LearnerMessage.LearnRequestState() ).setHeader(
                                                        InstanceId.INSTANCE,
                                                        id.toString() ) );
                                            }
                                        }
                                    }
                                }

                                // Set another timeout
                                context.setTimeout( "learn", Message.timeout( LearnerMessage.learnTimedout,
                                        message ) );
                            }
                            break;
                        }

                        case learnRequest:
                        {
                            // Someone wants to learn a value that we might have
                            InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstance( instanceId );
                            if ( instance.isState( PaxosInstance.State.closed ) ||
                                    instance.isState( PaxosInstance.State.delivered ) )
                            {
                                outgoing.offer( Message.respond( LearnerMessage.learn, message,
                                        new LearnerMessage.LearnState( instance.value_2 ) ).
                                        setHeader( InstanceId.INSTANCE, instanceId.toString() ).
                                        setHeader( Message.CONVERSATION_ID, instance.conversationIdHeader ) );
                            }
                            else
                            {
                                outgoing.offer( message.copyHeadersTo( Message.respond( LearnerMessage.learnFailed,
                                        message,
                                        new LearnerMessage.LearnFailedState() ), org.neo4j.cluster.protocol
                                  .atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case learnFailed:
                        {
                            InstanceId instanceId = new InstanceId( message );
                            context.notifyLearnMiss( instanceId );
                            break;
                        }

                        case catchUp:
                        {
                            long catchUpTo = message.<Long>getPayload();

                            if ( context.getLastKnownLearnedInstanceInCluster() < catchUpTo )
                            {
                                context.setNextInstanceId( catchUpTo + 1 );

                                // Try to get up to date
                                for ( long instanceId = context.getLastLearnedInstanceId() + 1;
                                      instanceId <= catchUpTo; instanceId++ )
                                {
                                    InstanceId id = new InstanceId( instanceId );
                                    PaxosInstance instance = context.getPaxosInstance( id );
                                    if ( !instance.isState( PaxosInstance.State.closed ) &&
                                            !instance.isState( PaxosInstance.State.delivered ) )
                                    {
                                        outgoing.offer( Message.to( LearnerMessage.learnRequest,
                                                lastKnownAliveUriOrSenderUri( context, message ),
                                                new LearnerMessage.LearnRequestState() ).setHeader(
                                                InstanceId.INSTANCE,
                                                id.toString() ) );
                                        context.setTimeout( "learn",
                                                Message.timeout( LearnerMessage.learnTimedout, message ) );
                                        break;
                                    }
                                }

                                org.neo4j.cluster.InstanceId instanceId =
                                        message.hasHeader( Message.INSTANCE_ID )
                                        ? new org.neo4j.cluster.InstanceId(
                                                Integer.parseInt( message.getHeader( Message.INSTANCE_ID ) ) )
                                        : context.getMyId();
                                context.setLastKnownLearnedInstanceInCluster( catchUpTo, instanceId );
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

                private URI lastKnownAliveUriOrSenderUri( LearnerContext context, Message<LearnerMessage> message )
                        throws URISyntaxException
                {
                    org.neo4j.cluster.InstanceId lastKnownAliveInstance = context.getLastKnownAliveUpToDateInstance();
                    if ( lastKnownAliveInstance != null )
                    {
                        return context.getUriForId( lastKnownAliveInstance );
                    } else
                    {
                        return new URI(message.getHeader( Message.FROM ));
                    }
                }
            }
}
