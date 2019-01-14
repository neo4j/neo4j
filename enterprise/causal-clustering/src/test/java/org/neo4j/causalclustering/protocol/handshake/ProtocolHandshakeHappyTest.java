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
package org.neo4j.causalclustering.protocol.handshake;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.GRATUITOUS_OBFUSCATION;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZ4;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZO;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * @see ProtocolHandshakeSadTest sad path tests
 */
@RunWith( Parameterized.class )
public class ProtocolHandshakeHappyTest
{
    @Parameterized.Parameter
    public Parameters parameters;

    @Parameterized.Parameters
    public static Collection<Parameters> data()
    {
        // Application protocols
        ApplicationSupportedProtocols allRaft =
                new ApplicationSupportedProtocols( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) );
        ApplicationSupportedProtocols raft1 =
                new ApplicationSupportedProtocols( RAFT, singletonList( RAFT_1.implementation() ) );
        ApplicationSupportedProtocols allRaftByDefault =
                new ApplicationSupportedProtocols( RAFT, emptyList() );

        // Modifier protocols
        Collection<ModifierSupportedProtocols> allModifiers = asList(
                new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ),
                new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) )
                );
        Collection<ModifierSupportedProtocols> allCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ) );
        Collection<ModifierSupportedProtocols> allObfuscationModifiers = singletonList(
                new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) ) );
        Collection<ModifierSupportedProtocols> allCompressionModifiersByDefault = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, emptyList() ) );

        List<ModifierSupportedProtocols> onlyLzoCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, singletonList( LZO.implementation() ) ) );
        List<ModifierSupportedProtocols> onlySnappyCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, singletonList( SNAPPY.implementation() ) ) );

        Collection<ModifierSupportedProtocols> noModifiers = emptyList();

        // Ordered modifier protocols
        ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( TestModifierProtocols.values(), allModifiers );
        String[] lzoFirstVersions = { LZO.implementation(), LZ4.implementation(), SNAPPY.implementation() };
        List<ModifierSupportedProtocols> lzoFirstCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, asList( lzoFirstVersions ) ) );
        ModifierProtocol preferredLzoFirstCompressionModifier =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( lzoFirstVersions ) ).get();

        String[] snappyFirstVersions = { SNAPPY.implementation(), LZ4.implementation(), LZO.implementation() };
        List<ModifierSupportedProtocols> snappyFirstCompressionModifiers = singletonList(
                new ModifierSupportedProtocols( COMPRESSION, asList( snappyFirstVersions ) ) );
        ModifierProtocol preferredSnappyFirstCompressionModifier =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( snappyFirstVersions ) ).get();

        return asList(
                // Everything
                new Parameters( allRaft, allRaft, allModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                // Application protocols
                new Parameters( allRaft, allRaftByDefault, allModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),
                new Parameters( allRaftByDefault, allRaft, allModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                new Parameters( allRaft, raft1, allModifiers, allModifiers, RAFT_1,
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),
                new Parameters( raft1, allRaft, allModifiers, allModifiers, RAFT_1,
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ), TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                // Modifier protocols
                new Parameters( allRaft, allRaft, allModifiers, allCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allCompressionModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allModifiers, allCompressionModifiersByDefault, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allCompressionModifiersByDefault, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.latest( COMPRESSION ) } ),
                new Parameters( allRaft, allRaft, allModifiers, allObfuscationModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {  TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),
                new Parameters( allRaft, allRaft, allObfuscationModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {  TestModifierProtocols.latest( GRATUITOUS_OBFUSCATION ) } ),

                // prioritisation
                new Parameters( allRaft, allRaft, allModifiers, lzoFirstCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { LZO } ),
                new Parameters( allRaft, allRaft, lzoFirstCompressionModifiers, allCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { preferredLzoFirstCompressionModifier } ),
                new Parameters( allRaft, allRaft, allModifiers, snappyFirstCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { SNAPPY } ),
                new Parameters( allRaft, allRaft, snappyFirstCompressionModifiers, allCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { preferredSnappyFirstCompressionModifier } ),

                // restriction
                new Parameters( allRaft, allRaft, allModifiers, onlyLzoCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.LZO } ),
                new Parameters( allRaft, allRaft, onlyLzoCompressionModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] { TestModifierProtocols.LZO } ),

                // incompatible
                new Parameters( allRaft, allRaft, onlySnappyCompressionModifiers, onlyLzoCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} ),
                new Parameters( allRaft, allRaft, onlyLzoCompressionModifiers, onlySnappyCompressionModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} ),

                // no modifiers
                new Parameters( allRaft, allRaft, allModifiers, noModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} ),
                new Parameters( allRaft, allRaft, noModifiers, allModifiers, TestApplicationProtocols.latest( RAFT ),
                        new ModifierProtocol[] {} )
                );
    }

    @Test
    public void shouldHandshakeApplicationProtocolOnClient()
    {
        // given
        Fixture fixture = new Fixture( parameters );

        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = fixture.initiate();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        ProtocolStack clientProtocolStack = clientHandshakeFuture.getNow( null );
        assertThat( clientProtocolStack.applicationProtocol(), equalTo( parameters.expectedApplicationProtocol ) );
    }

    @Test
    public void shouldHandshakeModifierProtocolsOnClient()
    {
        // given
        Fixture fixture = new Fixture( parameters );

        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = fixture.initiate();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        ProtocolStack clientProtocolStack = clientHandshakeFuture.getNow( null );
        if ( parameters.expectedModifierProtocols.length == 0 )
        {
            assertThat( clientProtocolStack.modifierProtocols(), empty() );
        }
        else
        {
            assertThat( clientProtocolStack.modifierProtocols(), contains( parameters.expectedModifierProtocols ) );
        }
    }

    @Test
    public void shouldHandshakeApplicationProtocolOnServer()
    {
        // given
        Fixture fixture = new Fixture( parameters );

        // when
        fixture.initiate();
        fixture.handshakeServer.protocolStackFuture();
        CompletableFuture<ProtocolStack> serverHandshakeFuture = fixture.handshakeServer.protocolStackFuture();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        ProtocolStack serverProtocolStack = serverHandshakeFuture.getNow( null );
        assertThat( serverProtocolStack.applicationProtocol(), equalTo( parameters.expectedApplicationProtocol ) );
    }

    @Test
    public void shouldHandshakeModifierProtocolsOnServer()
    {
        // given
        Fixture fixture = new Fixture( parameters );

        // when
        fixture.initiate();
        fixture.handshakeServer.protocolStackFuture();
        CompletableFuture<ProtocolStack> serverHandshakeFuture = fixture.handshakeServer.protocolStackFuture();

        // then
        assertFalse( fixture.clientChannel.isClosed() );
        ProtocolStack serverProtocolStack = serverHandshakeFuture.getNow( null );
        if ( parameters.expectedModifierProtocols.length == 0 )
        {
            assertThat( serverProtocolStack.modifierProtocols(), empty() );
        }
        else
        {
            assertThat( serverProtocolStack.modifierProtocols(), contains( parameters.expectedModifierProtocols ) );
        }
    }

    static class Fixture
    {
        final HandshakeClient handshakeClient;
        final HandshakeServer handshakeServer;
        final FakeChannelWrapper clientChannel;
        final ApplicationProtocolRepository clientApplicationProtocolRepository;
        final ModifierProtocolRepository clientModifierProtocolRepository;
        final Parameters parameters;

        Fixture( Parameters parameters )
        {
            ApplicationProtocolRepository serverApplicationProtocolRepository =
                    new ApplicationProtocolRepository( TestApplicationProtocols.values(), parameters.serverApplicationProtocol );
            ModifierProtocolRepository serverModifierProtocolRepository =
                    new ModifierProtocolRepository( TestModifierProtocols.values(), parameters.serverModifierProtocols );

            clientApplicationProtocolRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), parameters.clientApplicationProtocol );
            clientModifierProtocolRepository = new ModifierProtocolRepository( TestModifierProtocols.values(), parameters.clientModifierProtocols );

            handshakeClient = new HandshakeClient();
            handshakeServer = new HandshakeServer(
                    serverApplicationProtocolRepository,
                    serverModifierProtocolRepository,
                    new FakeServerChannel( handshakeClient ) );
            clientChannel = new FakeClientChannel( handshakeServer );
            this.parameters = parameters;
        }

        private CompletableFuture<ProtocolStack> initiate()
        {
            return handshakeClient.initiate( clientChannel, clientApplicationProtocolRepository, clientModifierProtocolRepository );
        }
    }

    static class Parameters
    {
        final ApplicationSupportedProtocols clientApplicationProtocol;
        final ApplicationSupportedProtocols serverApplicationProtocol;
        final Collection<ModifierSupportedProtocols> clientModifierProtocols;
        final Collection<ModifierSupportedProtocols> serverModifierProtocols;
        final ApplicationProtocol expectedApplicationProtocol;
        final ModifierProtocol[] expectedModifierProtocols;

        Parameters( ApplicationSupportedProtocols clientApplicationProtocol,
                ApplicationSupportedProtocols serverApplicationProtocol,
                Collection<ModifierSupportedProtocols> clientModifierProtocols,
                Collection<ModifierSupportedProtocols> serverModifierProtocols,
                ApplicationProtocol expectedApplicationProtocol,
                ModifierProtocol[] expectedModifierProtocols )
        {
            this.clientModifierProtocols = clientModifierProtocols;
            this.clientApplicationProtocol = clientApplicationProtocol;
            this.serverApplicationProtocol = serverApplicationProtocol;
            this.serverModifierProtocols = serverModifierProtocols;
            this.expectedApplicationProtocol = expectedApplicationProtocol;
            this.expectedModifierProtocols = expectedModifierProtocols;
        }
    }

    abstract static class FakeChannelWrapper implements Channel
    {
        private boolean closed;

        public boolean isDisposed()
        {
            return closed;
        }

        public void dispose()
        {
            closed = true;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        public abstract CompletableFuture<Void> write( Object msg );

        public CompletableFuture<Void> writeAndFlush( Object msg )
        {
            return write( msg );
        }

        boolean isClosed()
        {
            return closed;
        }
    }

    static class FakeServerChannel extends FakeChannelWrapper
    {
        private final HandshakeClient handshakeClient;

        FakeServerChannel( HandshakeClient handshakeClient )
        {
            super();
            this.handshakeClient = handshakeClient;
        }

        @Override
        public CompletableFuture<Void> write( Object msg )
        {
            ((ClientMessage) msg).dispatch( handshakeClient );
            return CompletableFuture.completedFuture( null );
        }
    }

    static class FakeClientChannel extends FakeChannelWrapper
    {
        private final HandshakeServer handshakeServer;

        FakeClientChannel( HandshakeServer handshakeServer )
        {
            super();
            this.handshakeServer = handshakeServer;
        }

        @Override
        public CompletableFuture<Void> write( Object msg )
        {
            ((ServerMessage) msg).dispatch( handshakeServer );
            return CompletableFuture.completedFuture( null );
        }
    }
}
