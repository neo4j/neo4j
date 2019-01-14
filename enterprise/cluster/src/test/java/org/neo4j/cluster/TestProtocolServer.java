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
package org.neo4j.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.statemachine.StateTransitionListener;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TODO
 */
public class TestProtocolServer
        implements MessageProcessor
{
    protected final TestMessageSource receiver;
    protected final TestMessageSender sender;

    protected ProtocolServer server;
    private final DelayedDirectExecutor stateMachineExecutor;
    private URI serverUri;

    public TestProtocolServer( LogProvider logProvider, TimeoutStrategy timeoutStrategy, ProtocolServerFactory factory, URI serverUri,
                               InstanceId instanceId, AcceptorInstanceStore acceptorInstanceStore,
                               ElectionCredentialsProvider electionCredentialsProvider )
    {
        this.serverUri = serverUri;
        this.receiver = new TestMessageSource();
        this.sender = new TestMessageSender();

        stateMachineExecutor = new DelayedDirectExecutor( logProvider );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );
        when( config.get( ClusterSettings.strict_initial_hosts) ).thenReturn( Boolean.FALSE );

        server = factory.newProtocolServer( instanceId, timeoutStrategy, receiver, sender, acceptorInstanceStore,
                electionCredentialsProvider, stateMachineExecutor, new ObjectStreamFactory(), new ObjectStreamFactory(),
                config );

        server.listeningAt( serverUri );
    }

    public ProtocolServer getServer()
    {
        return server;
    }

    public Timeouts getTimeouts()
    {
        return server.getTimeouts();
    }

    @Override
    public boolean process( Message message )
    {
        return receiver.process( message );
    }

    public void sendMessages( List<Message> output )
    {
        sender.sendMessages( output );
    }

    public <T> T newClient( Class<T> clientProxyInterface )
    {
        return server.newClient( clientProxyInterface );
    }

    public TestProtocolServer addStateTransitionListener( StateTransitionListener listener )
    {
        server.addStateTransitionListener( listener );
        return this;
    }

    public void tick( long now )
    {
        // Time passes - check timeouts
        server.getTimeouts().tick( now );

        stateMachineExecutor.drain();
    }

    @Override
    public String toString()
    {
        return server.getServerId() + ": " + sender.getMessages().size() + server.toString();
    }

    public class TestMessageSender implements MessageSender
    {
        List<Message> messages = new ArrayList<>();

        @Override
        public void process( List<Message<? extends MessageType>> messages )
        {
            for ( Message<? extends MessageType> message : messages )
            {
                process( message );
            }
        }

        @Override
        public boolean process( Message<? extends MessageType> message )
        {
            message.setHeader( Message.HEADER_FROM, serverUri.toASCIIString() );
            messages.add( message );
            return true;
        }

        public List<Message> getMessages()
        {
            return messages;
        }

        public void sendMessages( List<Message> output )
        {
            output.addAll( messages );
            messages.clear();
        }
    }

    public class TestMessageSource
            implements MessageSource, MessageProcessor
    {
        final Listeners<MessageProcessor> listeners = new Listeners<>();

        @Override
        public void addMessageProcessor( MessageProcessor listener )
        {
            listeners.add( listener );
        }

        @Override
        public boolean process( Message<? extends MessageType> message )
        {
            for ( MessageProcessor listener : listeners )
            {
                if ( !listener.process( message ) )
                {
                    return false;
                }
            }
            return true;
        }
    }
}
