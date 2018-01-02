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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.statemachine.State;

/**
 * State machine for Paxos Proposer
 */
public enum ProposerState
        implements State<ProposerContext, ProposerMessage>
{
    start
            {
                @Override
                public ProposerState handle( ProposerContext context,
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
                public ProposerState handle( ProposerContext context,
                                             Message<ProposerMessage> message,
                                             MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case propose:
                        {
                            propose( context, message, outgoing, determineAcceptorSet( message, context ) );
                            break;
                        }

                        case rejectPrepare:
                        {
                            // Denial of prepare
                            ProposerMessage.RejectPrepare rejectPropose = message.getPayload();
                            org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId = new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstance( instanceId );
                            context.getLog( ProposerState.class ).debug( "Propose for instance " + instance
                                    + " rejected from " + message.getHeader( Message.FROM ) + " with ballot "
                                    + rejectPropose.getBallot() );

                            if ( instance.isState( PaxosInstance.State.p1_pending ) &&
                                    instance.getBallot() < rejectPropose.getBallot()) // Ignore multiple rejects on same prepare
                            {
                                long ballot = instance.ballot;
                                while ( ballot <= rejectPropose.getBallot() )
                                {
                                    ballot += 1000; // Make sure we win next time
                                }

                                instance.phase1Timeout( ballot );
                                context.getLog( ProposerState.class ).debug(
                                        "Reproposing instance " + instance + " at ballot " + instance.ballot
                                                + " after rejectPrepare" );
                                for ( URI acceptor : instance.getAcceptors() )
                                {
                                    outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.prepare,
                                            acceptor, new AcceptorMessage.PrepareState( ballot ) ),
                                            org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                }
                                
                                assert instance.value_1 == null : "value_1 should have been null at this point";
                                Object payload = context.getBookedInstance( instanceId ).getPayload();
                                assert payload != null : "Should have a booked instance payload for " + instanceId;
                                // This will reset the phase1Timeout if existing
                                context.setTimeout( instanceId, message.copyHeadersTo( Message.timeout(
                                        ProposerMessage.phase1Timeout, message, payload ), org.neo4j.cluster.protocol
                                  .atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case phase1Timeout:
                        {
                            org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId = new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstance( instanceId );
                            if ( instance.isState( PaxosInstance.State.p1_pending ) )
                            {
                                if ( instance.ballot > 10000 )
                                {
                                    context.getLog( ProposerState.class ).warn( "Propose failed due to phase 1 " +
                                            "timeout" );

                                    // Fail this propose
                                    Message originalMessage = context.getBookedInstance( instance.id );
                                    // Also make sure that all headers are copied over
                                    outgoing.offer( originalMessage.copyHeadersTo(
                                            Message.internal( AtomicBroadcastMessage.failed,
                                                    originalMessage.getPayload() ) ) );
                                    context.cancelTimeout( instanceId );
                                }
                                else
                                {
                                    long ballot = instance.ballot + 1000;

                                    instance.phase1Timeout( ballot );

                                    for ( URI acceptor : instance.getAcceptors() )
                                    {
                                        outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.prepare,
                                                acceptor, new AcceptorMessage.PrepareState( ballot ) ),
                                                org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                    }
                                    context.setTimeout( instanceId, message.copyHeadersTo( Message.timeout(
                                            ProposerMessage.phase1Timeout, message, message.getPayload() ),
                                            org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                }
                            }
                            else if ( instance.isState( PaxosInstance.State.closed ) || instance.isState(
                                    PaxosInstance.State.delivered ) )
                            {
                                // Retry
                                Message oldMessage = context.unbookInstance( instance.id );
                                context.getLog( getClass() ).debug( "Retrying instance " + instance.id +
                                        " with message " + message.getPayload() +
                                        ". Previous instance was " + oldMessage );
                                outgoing.offer( Message.internal( ProposerMessage.propose, message.getPayload() ) );
                            }
                            break;
                        }

                        case promise:
                        {
                            // P
                            ProposerMessage.PromiseState promiseState = message.getPayload();
                            PaxosInstance instance = context.getPaxosInstance( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( message ) );

                            if ( instance.isState( PaxosInstance.State.p1_pending ) && instance.ballot ==
                                    promiseState.getBallot() )
                            {
                                instance.promise( promiseState );

                                if ( instance.isPromised( context.getMinimumQuorumSize( instance.getAcceptors() ) ) )
                                {
                                    context.cancelTimeout( instance.id );

                                    // No promises contained a value
                                    Object readyValue = instance.value_2 == null ?
                                            context.getBookedInstance( instance.id ).getPayload() : instance
                                            .value_2;
                                    if ( instance.value_1 == null )
                                    {
                                        // R0
                                        instance.ready( readyValue, true );
                                    }
                                    else
                                    {
                                        // R1
                                        if ( instance.value_2 == null )
                                        {
                                            // Another value was already associated with this instance. Push value
                                            // back onto pending list
                                            context.pendingValue( context.unbookInstance( instance.id ) );

                                            instance.ready( instance.value_1, false );
                                        }
                                        else if ( instance.value_1.equals( readyValue ) )
                                        {
                                            instance.ready( instance.value_2, instance.clientValue );
                                        }
                                        else if ( instance.clientValue )
                                        {
                                            // Another value was already associated with this instance. Push value
                                            // back onto pending list
                                            context.pendingValue( context.unbookInstance( instance.id ) );

                                            instance.ready( instance.value_1, false );
                                        }
                                        else
                                        {
                                            // Another value was already associated with this instance. Push value
                                            // back onto pending list
                                            context.pendingValue( context.unbookInstance( instance.id ) );
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
                                                        instance.value_2 ) ), org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                    }

                                    context.setTimeout( instance.id,
                                            message.copyHeadersTo( Message.timeout( ProposerMessage.phase2Timeout,
                                                    message, readyValue ), org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
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
                            org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId = new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstance( instanceId );

                            if ( instance.isState( PaxosInstance.State.p2_pending ) )
                            {
                                ProposerMessage.RejectAcceptState state = message.getPayload();
                                instance.rejected( state );

                                if ( !instance.isAccepted( context.getMinimumQuorumSize( instance.getAcceptors() ) ) )
                                {
                                    context.cancelTimeout( instanceId );

                                    context.getLog( ProposerState.class ).warn( "Accept rejected:" +
                                            instance.state );

                                    if ( instance.clientValue )
                                    {
                                        Message copyWithValue = Message.internal( ProposerMessage.propose,
                                                instance.value_2 );
                                        message.copyHeadersTo( copyWithValue );
                                        propose( context, copyWithValue, outgoing, instance.getAcceptors() );
                                    }
                                }
                            }
                            break;
                        }

                        case phase2Timeout:
                        {
                            org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId = new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstance( instanceId );

                            if ( instance.isState( PaxosInstance.State.p2_pending ) )
                            {
                                long ballot = instance.ballot + 1000;
                                instance.phase2Timeout( ballot );

                                for ( URI acceptor : instance.getAcceptors() )
                                {
                                    outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.prepare,
                                            acceptor, new AcceptorMessage.PrepareState( ballot ) ),
                                            org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                }

                                context.setTimeout( instanceId, message.copyHeadersTo( Message.timeout(
                                        ProposerMessage.phase1Timeout, message, message.getPayload() ),
                                        org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                            }
                            else if ( instance.isState( PaxosInstance.State.closed )
                                    || instance.isState( PaxosInstance.State.delivered ) )
                            {
                                outgoing.offer( message.copyHeadersTo( Message.internal( ProposerMessage.propose,
                                        message.getPayload() ) ) );
                            }
                            break;
                        }

                        case accepted:
                        {
                            PaxosInstance instance = context.getPaxosInstance( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId(
                                    message ) );

                            if ( instance.isState( PaxosInstance.State.p2_pending ) )
                            {
                                ProposerMessage.AcceptedState acceptedState = message.getPayload();
                                instance.accepted( acceptedState );

                                // Value has been accepted! Now distribute to all learners
                                if ( instance.accepts.size() >= context.getMinimumQuorumSize( instance.getAcceptors()
                                ) )
                                {
                                    context.cancelTimeout( instance.id );

                                    // Might have to extra-tell myself if not yet officially part of cluster
                                    if ( instance.value_2 instanceof ClusterMessage.ConfigurationChangeState )
                                    {
                                        context.patchBookedInstances( (ClusterMessage.ConfigurationChangeState) instance.value_2);

                                        ClusterMessage.ConfigurationChangeState state = (ClusterMessage
                                                .ConfigurationChangeState) instance.value_2;
                                        // TODO getLearners might return wrong list if another join happens at the
                                        // same time
                                        // Proper fix is to wait with this learn until we have learned all previous
                                        // configuration changes

                                        // TODO Fix this to use InstanceId instead of URI
                                        for ( URI learner : context.getMemberURIs() )
                                        {
                                            if ( learner.equals( context.boundAt() ) )
                                            {
                                                outgoing.offer( message.copyHeadersTo( Message.internal( LearnerMessage
                                                        .learn, new LearnerMessage.LearnState( instance.value_2 ) ),
                                                        org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                            }
                                            else
                                            {
                                                outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                        .learn, learner,
                                                        new LearnerMessage.LearnState( instance.value_2 ) ),
                                                        org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                            }
                                        }

                                        // Tell joiner of this cluster configuration change
                                        if ( state.getJoin() != null )
                                        {
                                            outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                    .learn, state.getJoinUri(),
                                                    new LearnerMessage.LearnState( instance.value_2 ) ),
                                                    org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                        }
                                    }
                                    else
                                    {
                                        // Tell learners
                                        for ( URI learner : context.getMemberURIs() )
                                        {
                                            outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                    .learn, learner,
                                                    new LearnerMessage.LearnState( instance.value_2 ) ),
                                                    org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) );
                                        }
                                    }

                                    context.unbookInstance( instance.id );

                                    // Check if we have anything pending - try to start process for it
                                    if ( context.hasPendingValues() && context.canBookInstance() )
                                    {
                                        Message proposeMessage = context.popPendingValue();
                                        context.getLog( ProposerState.class ).debug( "Restarting "
                                                + proposeMessage + " booked:"
                                                + context.nrOfBookedInstances() );
                                        outgoing.offer( proposeMessage );
                                    }
                                }
                            } else
                            {
                                context.getLog( ProposerState.class ).debug( "Instance receiving an accepted is in the wrong state:"+instance );
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
    };

    private static void propose( ProposerContext context, Message message, MessageHolder outgoing,
                                 List<URI> acceptors )
    {
        org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId instanceId;
        if ( message.hasHeader( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE ) )
        {
            instanceId = new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( message );
        }
        else
        {
            instanceId = context.newInstanceId();

            message.setHeader( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE, instanceId.toString() );
            context.bookInstance( instanceId, message );
        }

        long ballot = 1000 + context.getMyId().toIntegerIndex(); // First server will have first ballot id be 1001

        PaxosInstance instance = context.getPaxosInstance( instanceId );

        /*
         * If the instance already has an acceptor set, use that. This ensures that patched acceptor sets, for example,
         * are respected.
         */
        if ( instance.getAcceptors() != null )
        {
            acceptors = instance.getAcceptors();
        }

        if ( !(instance.isState( PaxosInstance.State.closed ) || instance.isState( PaxosInstance.State.delivered )) )
        {
            instance.propose( ballot, acceptors );

            for ( URI acceptor : acceptors )
            {
                outgoing.offer( Message.to( AcceptorMessage.prepare, acceptor, new AcceptorMessage.PrepareState(
                        ballot ) ).setHeader( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE, instanceId.toString() ) );
            }

            context.setTimeout( instanceId, Message.timeout( ProposerMessage.phase1Timeout, message,
                    message.getPayload() ).setHeader( org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE, instanceId.toString() ) );
        }
        else
        {
            // Wait with this value - we have our hands full right now
            context.pendingValue( message );
        }
    }

    private static List<URI> determineAcceptorSet( Message<ProposerMessage> message, ProposerContext context )
    {
        Object payload = message.getPayload();

        if ( payload instanceof ClusterMessage.ConfigurationChangeState )
        {
            ClusterMessage.ConfigurationChangeState state = message.getPayload();
            List<URI> acceptors = context.getAcceptors();

            Map<org.neo4j.cluster.InstanceId, URI> currentMembers = context.getMembers();

            // Never include node that is leaving
            if ( state.getLeave() != null )
            {
                acceptors = new ArrayList<URI>( acceptors );
                acceptors.remove( currentMembers.get(state.getLeave()) );
            }

            if ( state.getJoin() != null && currentMembers.containsKey( state.getJoin() ) )
            {
                acceptors.remove( currentMembers.get( state.getJoin() ) );
                if ( !acceptors.contains( state.getJoinUri() ) )
                {
                    acceptors.add( state.getJoinUri() );
                }
            }
            return acceptors;
        }
        else
        {
            return context.getAcceptors();
        }
    }
}
