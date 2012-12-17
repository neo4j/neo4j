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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.statemachine.State;

/**
 * State machine for Paxos Proposer
 */
public enum ProposerState
        implements State<MultiPaxosContext, ProposerMessage>
{
    start
            {
                @Override
                public ProposerState handle( MultiPaxosContext context,
                                             Message<ProposerMessage> message,
                                             MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case join:
                        {
                            return proposer;
                        }
                    }

                    return this;
                }
            },

    proposer
            {
                @Override
                public ProposerState handle( MultiPaxosContext context,
                                             Message<ProposerMessage> message,
                                             MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case propose:
                        {
                            Object payload = message.getPayload();

                            if ( payload instanceof ClusterMessage.ConfigurationChangeState )
                            {
                                ClusterMessage.ConfigurationChangeState state = message.getPayload();
                                List<URI> acceptors = context.getAcceptors();

                                // Never include node that is leaving
                                if ( state.getLeave() != null )
                                {
                                    acceptors = new ArrayList<URI>( acceptors );
                                    acceptors.remove( state.getLeave() );
                                }

                                propose( context, message, outgoing, payload, acceptors );
                            }
                            else
                            {
                                propose( context, message, outgoing, payload, context.getAcceptors() );
                            }

                            break;
                        }

                        case rejectPrepare:
                        {
                            // Denial of prepare
                            ProposerMessage.RejectPrepare rejectPropose = message.getPayload();
                            InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( instanceId );
                            if ( instance.isState( PaxosInstance.State.p1_pending ) )
                            {
                                long ballot = instance.ballot;
                                while ( ballot <= rejectPropose.getBallot() )
                                {
                                    ballot += 1000; // Make sure we win next time
                                }

                                instance.phase1Timeout( ballot, context.getAcceptors() );
                                for ( URI acceptor : instance.getAcceptors() )
                                {
                                    if ( acceptor.equals( context.clusterContext.getMe() ) )
                                    {

                                    }
                                    else
                                    {
                                        outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.prepare,
                                                acceptor, new AcceptorMessage.PrepareState( ballot ) ),
                                                InstanceId.INSTANCE ) );
                                    }
                                }
                                context.timeouts.setTimeout( instanceId, message.copyHeadersTo( Message.timeout(
                                        ProposerMessage
                                                .phase1Timeout, message ), InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case phase1Timeout:
                        {
                            InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( instanceId );
                            if ( instance.isState( PaxosInstance.State.p1_pending ) )
                            {
                                if ( instance.ballot > 10000 )
                                {
                                    context.clusterContext.getLogger( ProposerState.class ).warn( "Propose failed due" +
                                            " to phase 1 timeout" );

                                    // Fail this propose
                                    outgoing.offer( Message.internal( AtomicBroadcastMessage.failed,
                                            context.proposerContext.bookedInstances.get( instance.id ) ) );
                                }
                                else
                                {
                                    long ballot = instance.ballot + 1000;

                                    instance.phase1Timeout( ballot, context.getAcceptors() );

                                    for ( URI acceptor : instance.getAcceptors() )
                                    {
                                        outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.prepare,
                                                acceptor, new AcceptorMessage.PrepareState( ballot ) ),
                                                InstanceId.INSTANCE ) );
                                    }
                                    context.timeouts.setTimeout( instanceId, message.copyHeadersTo( Message.timeout(
                                            ProposerMessage
                                                    .phase1Timeout, message ), InstanceId.INSTANCE ) );
                                }
                            }
                            break;
                        }

                        case promise:
                        {
                            // P
                            ProposerMessage.PromiseState promiseState = message.getPayload();
                            PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( new InstanceId(
                                    message ) );

                            if ( instance.isState( PaxosInstance.State.p1_pending ) && instance.ballot ==
                                    promiseState.getBallot() )
                            {
                                instance.promise( promiseState );

                                if ( instance.isPromised( context.getMinimumQuorumSize( instance.getAcceptors() ) ) )
                                {
                                    context.timeouts.cancelTimeout( instance.id );

                                    // No promises contained a value
                                    if ( instance.value_1 == null )
                                    {
                                        // R0
                                        instance.ready( instance.value_2 == null ? context.proposerContext
                                                .bookedInstances.get( instance.id ) : instance.value_2, true );
                                    }
                                    else
                                    {
                                        // R1
                                        if ( instance.value_2 == null )
                                        {
                                            // Another value was already associated with this instance. Push value
                                            // back onto pending list
                                            context.proposerContext.pendingValues.offerFirst( context.proposerContext
                                                    .bookedInstances.remove( instance.id ) );

                                            instance.ready( instance.value_1, false );
                                        }
                                        else if ( instance.value_1.equals( instance.value_2 == null ? context
                                                .proposerContext.bookedInstances.get( instance.id ) : instance
                                                .value_2 ) )
                                        {
                                            instance.ready( instance.value_2, instance.clientValue );
                                        }
                                        else if ( instance.clientValue )
                                        {
                                            // Another value was already associated with this instance. Push value
                                            // back onto pending list
                                            context.proposerContext.pendingValues.offerFirst( context.proposerContext
                                                    .bookedInstances.remove( instance.id ) );

                                            instance.ready( instance.value_1, false );
                                        }
                                        else
                                        {
                                            // Another value was already associated with this instance. Push value
                                            // back onto pending list
                                            context.proposerContext.pendingValues.offerFirst( context.proposerContext
                                                    .bookedInstances.remove( instance.id ) );
                                            instance.ready( instance.value_1, false );
                                        }
                                    }

                                    // E: Send to Acceptors
                                    instance.pending();
                                    for ( URI acceptor : instance.getAcceptors() )
                                    {
                                        outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.accept,
                                                acceptor,
                                                new AcceptorMessage.AcceptState( instance.ballot,
                                                        instance.value_2 ) ), InstanceId.INSTANCE ) );
                                    }

                                    context.timeouts.setTimeout( instance.id,
                                            message.copyHeadersTo( Message.timeout( ProposerMessage.phase2Timeout,
                                                    message ), InstanceId.INSTANCE ) );
                                }
                                else
                                {
                                }
                            }
                            else
                            {
                            }
                            break;
                        }

                        case rejectAccept:
                        {
                            InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( instanceId );

                            if ( instance.isState( PaxosInstance.State.p2_pending ) )
                            {
                                ProposerMessage.RejectAcceptState state = message.getPayload();
                                instance.rejected( state );

                                if ( !instance.isAccepted( context.getMinimumQuorumSize( instance.getAcceptors() ) ) )
                                {
                                    context.timeouts.cancelTimeout( instanceId );

                                    context.clusterContext.getLogger( ProposerState.class ).warn( "Accept rejected:" +
                                            instance.state );

                                    if ( instance.clientValue )
                                    {
                                        propose( context, message, outgoing, instance.value_2,
                                                instance.getAcceptors() );
                                    }
                                }
                            }
                            break;
                        }

                        case phase2Timeout:
                        {
                            InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( instanceId );

                            if ( instance.isState( PaxosInstance.State.p2_pending ) )
                            {
                                long ballot = instance.ballot + 1000;
                                instance.phase2Timeout( ballot );

                                for ( URI acceptor : instance.getAcceptors() )
                                {
                                    outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.prepare,
                                            acceptor, new AcceptorMessage.PrepareState( ballot ) ),
                                            InstanceId.INSTANCE ) );
                                }

                                context.timeouts.setTimeout( instanceId, message.copyHeadersTo( Message.timeout(
                                        ProposerMessage.phase1Timeout, message ), InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case accepted:
                        {
                            PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( new InstanceId(
                                    message ) );

                            if ( instance.isState( PaxosInstance.State.p2_pending ) )
                            {
                                ProposerMessage.AcceptedState acceptedState = message.getPayload();
                                instance.accepted( acceptedState );

                                // Value has been accepted! Now distribute to all learners
                                if ( instance.accepts.size() == context.getMinimumQuorumSize( instance.getAcceptors()
                                ) )
                                {
                                    context.timeouts.cancelTimeout( instance.id );

                                    // Might have to extra-tell myself if not yet officially part of cluster
                                    if ( instance.value_2 instanceof ClusterMessage.ConfigurationChangeState )
                                    {
                                        ClusterMessage.ConfigurationChangeState state = (ClusterMessage
                                                .ConfigurationChangeState) instance.value_2;
                                        // TODO getLearners might return wrong list if another join happens at the
                                        // same time
                                        // Proper fix is to wait with this learn until we have learned all previous
                                        // configuration changes
                                        for ( URI learner : context.getLearners() )
                                        {
                                            outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                    .learn, learner,
                                                    new LearnerMessage.LearnState( instance.value_2 ) ),
                                                    InstanceId.INSTANCE ) );
                                        }

                                        // Tell joiner of this cluster configuration change
                                        if ( state.getJoin() != null )
                                        {
                                            outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                    .learn, state.getJoin(),
                                                    new LearnerMessage.LearnState( instance.value_2 ) ),
                                                    InstanceId.INSTANCE ) );
                                        }
                                    }
                                    else
                                    {
                                        // Tell learners
                                        for ( URI learner : context.getLearners() )
                                        {
                                            outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                    .learn, learner,
                                                    new LearnerMessage.LearnState( instance.value_2 ) ),
                                                    InstanceId.INSTANCE ) );
                                        }
                                    }

                                    context.proposerContext.bookedInstances.remove( instance.id );

                                    // Check if we have anything pending - try to start process for it
                                    if ( !context.proposerContext.pendingValues.isEmpty() && context.proposerContext
                                            .bookedInstances.size() < MAX_CONCURRENT_INSTANCES )
                                    {
                                        Object value = context.proposerContext.pendingValues.remove();
                                        context.clusterContext.getLogger( ProposerState.class ).debug( "Restarting "
                                                + value + " booked:"
                                                + context.proposerContext.bookedInstances.size() );
                                        outgoing.offer( Message.internal( ProposerMessage.propose, value ) );
                                    }
                                }
                                else
                                {
                                }
                            }
                            break;
                        }

                        case leave:
                        {
                            context.proposerContext.leave();
                            context.getPaxosInstances().leave();
                            return start;
                        }
                    }

                    return this;
                }

            };

    public final int MAX_CONCURRENT_INSTANCES = 10;

    private static void propose( MultiPaxosContext context, Message message, MessageHolder outgoing,
                                 Object payload, List<URI> acceptors )
    {
        InstanceId instanceId = context.proposerContext.newInstanceId( context.learnerContext
                .getLastKnownLearnedInstanceInCluster() );

        context.proposerContext.bookedInstances.put( instanceId, payload );

        long ballot = 1000 + context.getServerId(); // First server will have first ballot id be 1001

        PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( instanceId );

        if ( !(instance.isState( PaxosInstance.State.closed ) || instance.isState( PaxosInstance.State.delivered )) )
        {
            instance.propose( ballot, acceptors );

            for ( URI acceptor : acceptors )
            {
                outgoing.offer( Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState(
                        ballot ) ).setHeader( InstanceId.INSTANCE, instanceId.toString() ) );
            }

            context.timeouts.setTimeout( instanceId, Message.timeout( ProposerMessage.phase1Timeout, message,
                    instanceId ).setHeader( InstanceId.INSTANCE, instanceId.toString() ) );
        }
        else
        {
            // Wait with this value - we have our hands full right now
            context.proposerContext.pendingValues.offerFirst( payload );
        }
    }
}
