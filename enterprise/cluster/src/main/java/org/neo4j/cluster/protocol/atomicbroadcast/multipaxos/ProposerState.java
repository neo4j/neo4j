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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.kernel.impl.util.StringLogger;

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
                            propose( context, message, outgoing, determineAcceptorSet( message, context ) );
                            break;
                        }

                        case rejectPrepare:
                        {
                            // Denial of prepare
                            ProposerMessage.RejectPrepare rejectPropose = message.getPayload();
                            InstanceId instanceId = new InstanceId( message );
                            PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( instanceId );
                            context.clusterContext.getLogger( ProposerState.class ).debug(
                                    "Propose for instance " + instanceId + " at ballot " + instance.ballot
                                            + " rejected from " + message.getHeader( Message.FROM ) + " with ballot "
                                            + rejectPropose.getBallot() );

                            if ( instance.isState( PaxosInstance.State.p1_pending ) )
                            {
                                long ballot = instance.ballot;
                                while ( ballot <= rejectPropose.getBallot() )
                                {
                                    ballot += 1000; // Make sure we win next time
                                }

                                instance.phase1Timeout( ballot );
                                context.clusterContext.getLogger( ProposerState.class ).debug(
                                        "Reproposing instance " + instanceId + " at ballot " + instance.ballot
                                                + " after rejectPrepare");
                                for ( URI acceptor : instance.getAcceptors() )
                                {
                                    outgoing.offer( message.copyHeadersTo( Message.to( AcceptorMessage.prepare,
                                            acceptor, new AcceptorMessage.PrepareState( ballot ) ),
                                            InstanceId.INSTANCE ) );
                                }
                                // This will reset the phase1Timeout if existing
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
                                    context.clusterContext.getLogger( ProposerState.class ).warn( "Propose failed due to phase 1 timeout" );

                                    // Fail this propose
                                    Message originalMessage = context.proposerContext.bookedInstances.get( instance.id );
                                    // Also make sure that all headers are copied over
                                    outgoing.offer( originalMessage.copyHeadersTo(
                                            Message.internal( AtomicBroadcastMessage.failed,
                                                    originalMessage.getPayload() ) ) );
                                    context.timeouts.cancelTimeout( instanceId );
                                }
                                else
                                {
                                    long ballot = instance.ballot + 1000;

                                    instance.phase1Timeout( ballot );

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
                                                .bookedInstances.get( instance.id ).getPayload() : instance.value_2, true );
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
                                                .proposerContext.bookedInstances.get( instance.id ).getPayload() : instance
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
                                        Message copyWithValue = Message.internal( ProposerMessage.propose, instance.value_2 );
                                        message.copyHeadersTo( copyWithValue );
                                        propose( context, copyWithValue, outgoing, instance.getAcceptors() );
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
                                        patchBookedInstances( (ClusterMessage.ConfigurationChangeState) instance.value_2,
                                                context.proposerContext.bookedInstances, context.getPaxosInstances(),
                                                context.clusterContext.getConfiguration().getMembers(),
                                                context.clusterContext.getLogger( ProposerState.class ) );

                                        ClusterMessage.ConfigurationChangeState state = (ClusterMessage
                                                .ConfigurationChangeState) instance.value_2;
                                        // TODO getLearners might return wrong list if another join happens at the
                                        // same time
                                        // Proper fix is to wait with this learn until we have learned all previous
                                        // configuration changes

                                        // TODO Fix this to use InstanceId instead of URI
                                        for ( URI learner : context.getLearners() )
                                        {
                                            if ( learner.equals( context.clusterContext.boundAt() ) )
                                            {
                                                outgoing.offer( message.copyHeadersTo( Message.internal( LearnerMessage
                                                        .learn, new LearnerMessage.LearnState( instance.value_2 ) ),
                                                        InstanceId.INSTANCE ) );
                                            }
                                            else
                                            {
                                                outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                        .learn, learner,
                                                        new LearnerMessage.LearnState( instance.value_2 ) ),
                                                        InstanceId.INSTANCE ) );
                                            }
                                        }

                                        // Tell joiner of this cluster configuration change
                                        if ( state.getJoin() != null )
                                        {
                                            outgoing.offer( message.copyHeadersTo( Message.to( LearnerMessage
                                                    .learn, state.getJoinUri(),
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
                                        Message proposeMessage = context.proposerContext.pendingValues.remove();
                                        context.clusterContext.getLogger( ProposerState.class ).debug( "Restarting "
                                                + proposeMessage + " booked:"
                                                + context.proposerContext.bookedInstances.size() );
                                        outgoing.offer( proposeMessage );
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

    /**
     * This patches the booked instances that are pending in case the configuration of the cluster changes. This
     * should be called only when we learn a ConfigurationChangeState i.e. when we receive an accepted for
     * such a message. This won't "learn" the message, as in applying it on the cluster configuration, but will
     * just update properly the set of acceptors for pending instances.
     */
    private static void patchBookedInstances( ClusterMessage.ConfigurationChangeState value_2,
                                              Map<InstanceId, Message> bookedInstances,
                                              PaxosInstanceStore paxosInstances,
                                              Map<org.neo4j.cluster.InstanceId, URI> members,
                                              StringLogger logger )
    {
        if ( value_2.getJoin() != null )
        {
            for ( InstanceId instanceId : bookedInstances.keySet() )
            {
                PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                if ( instance.getAcceptors() != null)
                {
                    instance.getAcceptors().remove( members.get( value_2.getJoin() ) );

                    logger.debug( "For booked instance " + instance + " removed gone member "
                            + members.get( value_2.getJoin() ) + " added joining member "+ value_2.getJoinUri() );

                    if ( !instance.getAcceptors().contains(  value_2.getJoinUri() ) )
                    {
                        instance.getAcceptors().add( value_2.getJoinUri() );
                    }
                }
            }
        }
        else if ( value_2.getLeave() != null )
        {
            for ( InstanceId instanceId : bookedInstances.keySet() )
            {
                PaxosInstance instance = paxosInstances.getPaxosInstance( instanceId );
                if ( instance.getAcceptors() != null )
                {
                    logger.debug( "For booked instance " + instance + " removed leaving member "
                            + value_2.getLeave() + " (at URI " + members.get( value_2.getLeave() ) + ")" );
                    instance.getAcceptors().remove( members.get( value_2.getLeave() ) );
                }
            }
        }
    }

    public final int MAX_CONCURRENT_INSTANCES = 10;

    private static void propose( MultiPaxosContext context, Message message, MessageHolder outgoing,
                                 List<URI> acceptors )
    {
        InstanceId instanceId;
        if ( message.hasHeader( InstanceId.INSTANCE ) )
        {
            instanceId = new InstanceId( message );
        }
        else
        {
            instanceId = context.proposerContext.newInstanceId( context.learnerContext
                    .getLastKnownLearnedInstanceInCluster() );

            message.setHeader( InstanceId.INSTANCE, instanceId.toString() );
            context.proposerContext.bookedInstances.put( instanceId, message );
        }

        long ballot = 1000 + context.getServerId(); // First server will have first ballot id be 1001

        PaxosInstance instance = context.getPaxosInstances().getPaxosInstance( instanceId );

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
                        ballot ) ).setHeader( InstanceId.INSTANCE, instanceId.toString() ) );
            }

            context.timeouts.setTimeout( instanceId, Message.timeout( ProposerMessage.phase1Timeout, message,
                    instanceId ).setHeader( InstanceId.INSTANCE, instanceId.toString() ) );
        }
        else
        {
            // Wait with this value - we have our hands full right now
            context.proposerContext.pendingValues.offerFirst( message );
        }
    }

    private static List<URI> determineAcceptorSet( Message<ProposerMessage> message, MultiPaxosContext context )
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
                acceptors.remove( state.getLeave() );
            }

            if ( state.getJoin() != null && currentMembers.containsKey( state.getJoin() ))
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
