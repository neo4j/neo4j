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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.logging.Log;

import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext.LEARN_GAP_THRESHOLD;

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
                {
                    if ( message.getMessageType() == LearnerMessage.join )
                    {
                        return learner;
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
                ) throws IOException, ClassNotFoundException, URISyntaxException
                {
                    switch ( message.getMessageType() )
                    {
                        case learn:
                        {
                            LearnerMessage.LearnState learnState = message.getPayload();
                            final InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstance( instanceId );

                            Log log = context.getLog( getClass() );

                            // Skip if we already know about this
                            if ( instanceId.getId() <= context.getLastDeliveredInstanceId() )
                            {
                                break;
                            }

                            context.learnedInstanceId( instanceId.getId() );

                            instance.closed( learnState.getValue(), message.getHeader( Message.HEADER_CONVERSATION_ID ) );

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
                                        "Learned and closed instance " + instance.id +
                                                " from conversation " +
                                                instance.conversationIdHeader +
                                                " and the content was " +
                                                description );
                            }

                            /*
                             * Here we have to deal with a potential problem, in essence the fallout of a bug
                             * that happens elsewhere.
                             * The instance we just received should be delivered if it's the next one being waited on.
                             * But, some previous instance may not have closed yet and, more often than not, it may
                             * never close. That's the bug we're dealing with and the reasons that this happens are
                             * currently unknown. When this happens, all subsequent instances, including for example
                             * configuration updates of instances joining the cluster, will get stuck and not be
                             * applied, making everyone just hang around and not allow members to join or leave.
                             * If this situation arises, we have a way out. If the current master doesn't have the
                             * instance, then no one does or at least it's relatively safe to assume so. This
                             * assumption allows us to accept that if more than N instances are missing from this one,
                             * just go ahead and skip them and deliver, in order, everything that is currently pending.
                             * This is nothing more than a tradeoff between sticking with correctness and risking
                             * unavailability and risking correctness to ensure availability. Given that most
                             * pending messages are cluster configuration changes and these are idempontent, the risk
                             * is acceptable.
                             *
                             * Technically, what is going to happen here is that if we are the coordinator and the
                             * instance we just closed is larger than a threshold compared to the last delivered one,
                             * we'll just deliver everything that we have pending, potentially skipping some instances.
                             * Slaves will not do that and instead ask the master for what they're missing. Eventually
                             * the master will become unstuck and everyone will progress.
                             *
                             * Nevertheless, the normal case is what the non coordinators do, so we only enter the
                             * pathological handling code only when we are the coordinator AND we don't follow the
                             * happy path.
                             */
                            if ( context.isMe( context.getCoordinator() )
                                    && instanceId.getId() != context.getLastDeliveredInstanceId() + 1 )
                            {
                                context.getLog( LearnerState.class ).debug( "Gap developed in delivered instances," +
                                                "latest received was %s but last delivered was %d.",
                                        instanceId, context.getLastDeliveredInstanceId() );
                                /*
                                 * We'll wait a bit, eagerness should not cause out of order delivery. At the
                                 * same time, we have to make sure that if we get here, then we sort of erase any
                                 * issues accumulated so far, therefor we will try to deliver everything pending, in
                                 * order, regardless of whether we have it or not. In essence, we assume this is the
                                 * latest message and it is valid.
                                  */
                                if ( instanceId.getId() > context.getLastDeliveredInstanceId() + LEARN_GAP_THRESHOLD )
                                {
                                    context.getLog( LearnerState.class ).debug(
                                            "Gap threshold reached (%d), proceeding to deliver everything pending " +
                                                    "up until now", LEARN_GAP_THRESHOLD );
                                    boolean currentInstanceFound = false; // To assert we delivered this instance
                                    long checkInstanceId = context.getLastDeliveredInstanceId() + 1;
                                    final long startingInstanceId = checkInstanceId; // for debug message, later
                                    while ( ( instance = context.getPaxosInstance( new InstanceId(
                                            checkInstanceId ) ) ) != null ) // As long as it exists, deliver it
                                    {
                                        if ( checkInstanceId == instanceId.getId() )
                                        {
                                            currentInstanceFound = true;
                                        }
                                        instance.delivered();
                                        context.setLastDeliveredInstanceId( checkInstanceId );
                                        Message<AtomicBroadcastMessage> learnMessage = Message.internal(
                                                AtomicBroadcastMessage.broadcastResponse, instance.value_2 )
                                                .setHeader( InstanceId.INSTANCE, instance.id.toString() )
                                                .setHeader( Message.HEADER_CONVERSATION_ID, instance.conversationIdHeader );
                                        outgoing.offer( learnMessage );
                                        checkInstanceId++;
                                    }
                                    context.getLog( LearnerMessage.LearnState.class ).
                                            debug( "Delivered everything from %d up until %d. Triggering message was %s, delivered: %b",
                                                    startingInstanceId, checkInstanceId - 1, instanceId, currentInstanceFound );
                                }
                            }

                            /*
                             * Else we are a follower. Then we must wait for everything and deliver everything in order,
                             * relying on the master to hand out stuff we're missing. If the leader doesn't have it,
                             * then a restart may be necessary, because we cannot risk having more than one place in
                             * the cluster where decisions about skipping Paxos instances are taken.
                             */
                            else
                            {
                                if ( instanceId.getId() == context.getLastDeliveredInstanceId() + 1 )
                                {
                                    instance.delivered();
                                    outgoing.offer( Message.internal( AtomicBroadcastMessage.broadcastResponse,
                                            learnState.getValue() )
                                            .setHeader( InstanceId.INSTANCE, instance.id.toString() )
                                            .setHeader( Message.HEADER_CONVERSATION_ID, instance.conversationIdHeader ) );
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
                                                .setHeader( Message.HEADER_CONVERSATION_ID, instance.conversationIdHeader );
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
                                        setHeader( Message.HEADER_CONVERSATION_ID, instance.conversationIdHeader ) );
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
                                        message.hasHeader( Message.HEADER_INSTANCE_ID )
                                        ? new org.neo4j.cluster.InstanceId(
                                                Integer.parseInt( message.getHeader( Message.HEADER_INSTANCE_ID ) ) )
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

                        default:
                            break;
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
                    }
                    else
                    {
                        return new URI( message.getHeader( Message.HEADER_FROM ) );
                    }
                }
            }
}
