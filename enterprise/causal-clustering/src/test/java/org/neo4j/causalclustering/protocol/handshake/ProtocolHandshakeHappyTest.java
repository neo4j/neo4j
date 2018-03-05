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
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolIdentifier.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier.COMPRESSION;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION;
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
        SupportedProtocols<ApplicationProtocol> allRaft =
                new SupportedProtocols<>( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) );
        SupportedProtocols<ApplicationProtocol> raft1 =
                new SupportedProtocols<>( RAFT, singletonList( RAFT_1.version() ) );
        SupportedProtocols<ApplicationProtocol> allRaftByDefault =
                new SupportedProtocols<>( RAFT, emptyList() );

        // Modifier protocols
        Collection<SupportedProtocols<ModifierProtocol>> allModifiers = asList(
                new SupportedProtocols<>( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ),
                new SupportedProtocols<>( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) )
                );
        Collection<SupportedProtocols<ModifierProtocol>> allCompressionModifiers = singletonList(
                new SupportedProtocols<>( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ) );
        Collection<SupportedProtocols<ModifierProtocol>> allObfuscationModifiers = singletonList(
                new SupportedProtocols<>( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) ) );
        Collection<SupportedProtocols<ModifierProtocol>> allCompressionModifiersByDefault = singletonList(
                new SupportedProtocols<>( COMPRESSION, emptyList() ) );

        List<SupportedProtocols<ModifierProtocol>> onlyLzoCompressionModifiers = singletonList(
                new SupportedProtocols<>( COMPRESSION, singletonList( LZO.version() ) ) );
        List<SupportedProtocols<ModifierProtocol>> onlySnappyCompressionModifiers = singletonList(
                new SupportedProtocols<>( COMPRESSION, singletonList( SNAPPY.version() ) ) );

        Collection<SupportedProtocols<ModifierProtocol>> noModifiers = emptyList();

        // Ordered modifier protocols
        ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( TestModifierProtocols.values(), allModifiers );
        Integer[] lzoFirstVersions = { LZO.version(), LZ4.version(), SNAPPY.version() };
        List<SupportedProtocols<ModifierProtocol>> lzoFirstCompressionModifiers = singletonList(
                new SupportedProtocols<>( COMPRESSION, asList( lzoFirstVersions ) ) );
        ModifierProtocol preferredLzoFirstCompressionModifier =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( lzoFirstVersions ) ).get();

        Integer[] snappyFirstVersions = { SNAPPY.version(), LZ4.version(), LZO.version() };
        List<SupportedProtocols<ModifierProtocol>> snappyFirstCompressionModifiers = singletonList(
                new SupportedProtocols<>( COMPRESSION, asList( snappyFirstVersions ) ) );
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
    public void shouldHandshakeApplicationProtocolOnClient() throws Throwable
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
    public void shouldHandshakeModifierProtocolsOnClient() throws Throwable
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
    public void shouldHandshakeApplicationProtocolOnServer() throws Throwable
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
    public void shouldHandshakeModifierProtocolsOnServer() throws Throwable
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
        final SupportedProtocols<ApplicationProtocol> clientApplicationProtocol;
        final SupportedProtocols<ApplicationProtocol> serverApplicationProtocol;
        final Collection<SupportedProtocols<ModifierProtocol>> clientModifierProtocols;
        final Collection<SupportedProtocols<ModifierProtocol>> serverModifierProtocols;
        final ApplicationProtocol expectedApplicationProtocol;
        final ModifierProtocol[] expectedModifierProtocols;

        Parameters( SupportedProtocols<ApplicationProtocol> clientApplicationProtocol,
                SupportedProtocols<ApplicationProtocol> serverApplicationProtocol,
                Collection<SupportedProtocols<ModifierProtocol>> clientModifierProtocols,
                Collection<SupportedProtocols<ModifierProtocol>> serverModifierProtocols,
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
