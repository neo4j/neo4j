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
package org.neo4j.ha.correctness;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.DelayedDirectExecutor;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.MultiPaxosServerFactory;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InMemoryAcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.election.ElectionMessage;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.protocol.snapshot.SnapshotContext;
import org.neo4j.cluster.protocol.snapshot.SnapshotMessage;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighAvailabilityMemberInfoProvider;
import org.neo4j.kernel.ha.cluster.DefaultElectionCredentialsProvider;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClusterInstance
{
    private final Executor stateMachineExecutor;
    private final LogProvider logging;
    private final MultiPaxosServerFactory factory;
    private final ProtocolServer server;
    private final MultiPaxosContext ctx;
    private final InMemoryAcceptorInstanceStore acceptorInstanceStore;
    private final ProverTimeouts timeouts;
    private final ClusterInstanceInput input;
    private final ClusterInstanceOutput output;
    private final URI uri;

    public static final Executor DIRECT_EXECUTOR = Runnable::run;

    private boolean online = true;

    public static ClusterInstance newClusterInstance( InstanceId id, URI uri, Monitors monitors,
                                                      ClusterConfiguration configuration,
                                                      int maxSurvivableFailedMembers,
                                                      LogProvider logging )
    {
        MultiPaxosServerFactory factory = new MultiPaxosServerFactory( configuration,
                logging, monitors.newMonitor( StateMachines.Monitor.class ) );

        ClusterInstanceInput input = new ClusterInstanceInput();
        ClusterInstanceOutput output = new ClusterInstanceOutput( uri );

        ObjectStreamFactory objStreamFactory = new ObjectStreamFactory();

        ProverTimeouts timeouts = new ProverTimeouts( uri );

        InMemoryAcceptorInstanceStore acceptorInstances = new InMemoryAcceptorInstanceStore();

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( maxSurvivableFailedMembers );

        DelayedDirectExecutor executor = new DelayedDirectExecutor( logging );
        final MultiPaxosContext context = new MultiPaxosContext( id,
                Iterables.iterable( new ElectionRole( ClusterConfiguration.COORDINATOR ) ),
                new ClusterConfiguration( configuration.getName(), logging,
                        configuration.getMemberURIs() ),
                executor, logging, objStreamFactory, objStreamFactory, acceptorInstances, timeouts,
                new DefaultElectionCredentialsProvider( id, new StateVerifierLastTxIdGetter(),
                        new MemberInfoProvider() ), config
        );
        context.getClusterContext().setBoundAt( uri );

        SnapshotContext snapshotContext = new SnapshotContext( context.getClusterContext(),
                context.getLearnerContext() );

        DelayedDirectExecutor taskExecutor = new DelayedDirectExecutor( logging );
        ProtocolServer ps = factory.newProtocolServer(
                id, input, output, DIRECT_EXECUTOR, taskExecutor, timeouts, context, snapshotContext );

        return new ClusterInstance( DIRECT_EXECUTOR, logging, factory, ps, context, acceptorInstances, timeouts,
                input, output, uri );
    }

    ClusterInstance( Executor stateMachineExecutor, LogProvider logging, MultiPaxosServerFactory factory,
                            ProtocolServer server,
                            MultiPaxosContext ctx, InMemoryAcceptorInstanceStore acceptorInstanceStore,
                            ProverTimeouts timeouts, ClusterInstanceInput input, ClusterInstanceOutput output,
                            URI uri )
    {
        this.stateMachineExecutor = stateMachineExecutor;
        this.logging = logging;
        this.factory = factory;
        this.server = server;
        this.ctx = ctx;
        this.acceptorInstanceStore = acceptorInstanceStore;
        this.timeouts = timeouts;
        this.input = input;
        this.output = output;
        this.uri = uri;
    }

    public InstanceId id()
    {
        return server.getServerId();
    }

    /**
     * Process a message, returns all messages generated as output.
     */
    public Iterable<Message<? extends MessageType>> process( Message<? extends MessageType> message )
    {
        if ( online )
        {
            input.process( message );
            return output.messages();
        }
        else
        {
            return Iterables.empty();
        }
    }

    @Override
    public String toString()
    {
        return "[" + id() + ":" + Iterables.toString( stateMachineStates(), "," ) + "]";
    }

    private Iterable<String> stateMachineStates()
    {
        return Iterables.map( stateMachine -> stateMachine.getState().toString(),
                server.getStateMachines().getStateMachines() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ClusterInstance that = (ClusterInstance) o;

        if ( !toString().equals( that.toString() ) )
        {
            return false;
        }

        if ( !uri.equals( that.uri ) )
        {
            return false;
        }

        // TODO: For now, we only look at the states of the underlying state machines,
        // and ignore, at our peril, the MultiPaxosContext as part of this equality checks.
        // This means the prover ignores lots of possible paths it could generate, as it considers two
        // machines with different multi paxos state potentially equal and will ignore exploring both.
        // This should be undone as soon as possible. It's here because we need a better mechanism than
        // .equals() to compare that two contexts are the same, which is not yet implemented.

        return true;
    }

    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }

    private StateMachine snapshotStateMachine( LogProvider logProvider, MultiPaxosContext snapshotCtx, StateMachine
            stateMachine )
    {
        // This is done this way because all the state machines are sharing one piece of global state
        // (MultiPaxosContext), which is snapshotted as one coherent component. This means the state machines
        // cannot snapshot themselves, an external service needs to snapshot the full shared state and then create
        // new state machines sharing that state.

        Object ctx;
        Class<? extends MessageType> msgType = stateMachine.getMessageType();
        if ( msgType == AtomicBroadcastMessage.class )
        {
            ctx = snapshotCtx.getAtomicBroadcastContext();
        }
        else if ( msgType == AcceptorMessage.class )
        {
            ctx = snapshotCtx.getAcceptorContext();
        }
        else if ( msgType == ProposerMessage.class )
        {
            ctx = snapshotCtx.getProposerContext();
        }
        else if ( msgType == LearnerMessage.class )
        {
            ctx = snapshotCtx.getLearnerContext();
        }
        else if ( msgType == HeartbeatMessage.class )
        {
            ctx = snapshotCtx.getHeartbeatContext();
        }
        else if ( msgType == ElectionMessage.class )
        {
            ctx = snapshotCtx.getElectionContext();
        }
        else if ( msgType == SnapshotMessage.class )
        {
            ctx = new SnapshotContext( snapshotCtx.getClusterContext(), snapshotCtx.getLearnerContext() );
        }
        else if ( msgType == ClusterMessage.class )
        {
            ctx = snapshotCtx.getClusterContext();
        }
        else
        {
            throw new IllegalArgumentException( "I don't know how to snapshot this state machine: " + stateMachine );
        }
        return new StateMachine( ctx, stateMachine.getMessageType(), stateMachine.getState(), logProvider );
    }

    public ClusterInstance newCopy()
    {
        // A very invasive method of cloning a protocol server. Nonetheless, since this is mostly an experiment at this
        // point, it seems we can refactor later on to have a cleaner clone mechanism.
        // Because state machines share state, and are simultaneously conceptually unaware of each other, implementing
        // a clean snapshot mechanism is very hard. I've opted for having a dirty one here in the test code rather
        // than introducing a hack into the runtime code.

        ProverTimeouts timeoutsSnapshot = timeouts.snapshot();
        InMemoryAcceptorInstanceStore snapshotAcceptorInstances = acceptorInstanceStore.snapshot();

        ClusterInstanceOutput output = new ClusterInstanceOutput( uri );
        ClusterInstanceInput input = new ClusterInstanceInput();

        DelayedDirectExecutor executor = new DelayedDirectExecutor( logging );

        ObjectStreamFactory objectStreamFactory = new ObjectStreamFactory();
        MultiPaxosContext snapshotCtx = ctx.snapshot( logging, timeoutsSnapshot, executor, snapshotAcceptorInstances,
                objectStreamFactory, objectStreamFactory,
                new DefaultElectionCredentialsProvider( server.getServerId(), new StateVerifierLastTxIdGetter(),
                        new MemberInfoProvider() )
        );

        List<StateMachine> snapshotMachines = new ArrayList<>();
        for ( StateMachine stateMachine : server.getStateMachines().getStateMachines() )
        {
            snapshotMachines.add( snapshotStateMachine( logging, snapshotCtx, stateMachine ) );
        }

        ProtocolServer snapshotProtocolServer = factory.constructSupportingInfrastructureFor( server.getServerId(),
                input, output, executor, timeoutsSnapshot, stateMachineExecutor,
                snapshotCtx, snapshotMachines.toArray( new StateMachine[0] ) );

        return new ClusterInstance( stateMachineExecutor, logging, factory, snapshotProtocolServer, snapshotCtx,
                snapshotAcceptorInstances, timeoutsSnapshot, input, output, uri );
    }

    public URI uri()
    {
        return uri;
    }

    public boolean hasPendingTimeouts()
    {
        return timeouts.hasTimeouts();
    }

    public ClusterAction popTimeout()
    {
        return timeouts.pop();
    }

    /**
     * Make this instance stop responding to calls, and cancel all pending timeouts.
     */
    public void crash()
    {
        timeouts.cancelAllTimeouts();
        this.online = false;
    }

    private static class ClusterInstanceInput implements MessageSource, MessageProcessor
    {
        private final List<MessageProcessor> processors = new ArrayList<>();

        @Override
        public boolean process( Message<? extends MessageType> message )
        {
            for ( MessageProcessor processor : processors )
            {
                if ( !processor.process( message ) )
                {
                    return false;
                }
            }

            return true;
        }

        @Override
        public void addMessageProcessor( MessageProcessor messageProcessor )
        {
            processors.add( messageProcessor );
        }
    }

    private static class ClusterInstanceOutput implements MessageSender
    {
        private final List<Message<? extends MessageType>> messages = new ArrayList<>();
        private final URI uri;

        ClusterInstanceOutput( URI uri )
        {
            this.uri = uri;
        }

        @Override
        public boolean process( Message<? extends MessageType> message )
        {
            messages.add( message.setHeader( Message.HEADER_FROM, uri.toASCIIString() ) );
            return true;
        }

        @Override
        public void process( List<Message<? extends MessageType>> msgList )
        {
            for ( Message<? extends MessageType> msg : msgList )
            {
                process( msg );
            }
        }

        public Iterable<Message<? extends MessageType>> messages()
        {
            return messages;
        }
    }

    static class MemberInfoProvider implements HighAvailabilityMemberInfoProvider
    {
        @Override
        public HighAvailabilityMemberState getHighAvailabilityMemberState()
        {
            throw new UnsupportedOperationException( "TODO" );
        }
    }

    // TODO: Make this emulate commits happening
    static class StateVerifierLastTxIdGetter implements LastTxIdGetter
    {
        @Override
        public long getLastTxId()
        {
            return 0;
        }
    }
}
