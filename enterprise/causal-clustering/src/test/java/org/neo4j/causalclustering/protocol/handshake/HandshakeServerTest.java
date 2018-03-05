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

import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.neo4j.helpers.collection.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolIdentifier.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier.COMPRESSION;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.FAILURE;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.ROT13;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * @see HandshakeServerEnsureMagicTest
 */
public class HandshakeServerTest
{
    private Channel channel = mock( Channel.class );
    private SupportedProtocols<ApplicationProtocol> supportedApplicationProtocol =
            new SupportedProtocols<>( RAFT, emptyList() );
    private Collection<SupportedProtocols<ModifierProtocol>> supportedModifierProtocols = asList(
            new SupportedProtocols<>( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ),
            new SupportedProtocols<>( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) )
    );
    private ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedApplicationProtocol );
    private ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), supportedModifierProtocols );

    private HandshakeServer server =
            new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

    @Test
    public void shouldDeclineUnallowedApplicationProtocol()
    {
        // given
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle(
                new ApplicationProtocolRequest( TestApplicationProtocols.CATCHUP_1.identifier(), asSet( TestApplicationProtocols.CATCHUP_1.version() ) ) );

        // then
        verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackOnUnallowedApplicationProtocol()
    {
        // given
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle(
                new ApplicationProtocolRequest( TestApplicationProtocols.CATCHUP_1.identifier(), asSet( TestApplicationProtocols.CATCHUP_1.version() ) ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldDisconnectOnWrongMagicValue()
    {
        // when
        server.handle( new InitialMagicMessage( "PLAIN_VALUE" ) );

        // then
        verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackOnWrongMagicValue()
    {
        // when
        server.handle( new InitialMagicMessage( "PLAIN_VALUE" ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldAcceptCorrectMagicValue()
    {
        // when
        server.handle( InitialMagicMessage.instance() );

        // then
        assertUnfinished();
    }

    @Test
    public void shouldSendApplicationProtocolResponseForKnownProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ApplicationProtocolResponse( SUCCESS, TestApplicationProtocols.RAFT_3.identifier(), TestApplicationProtocols.RAFT_3.version() ) );
    }

    @Test
    public void shouldNotCloseConnectionIfKnownApplicationProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), versions ) );

        // then
        assertUnfinished();
    }

    @Test
    public void shouldSendNegativeResponseAndCloseForUnknownApplicationProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new ApplicationProtocolRequest( "UNKNOWN", versions ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( ApplicationProtocolResponse.NO_PROTOCOL );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackForUnknownApplicationProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new ApplicationProtocolRequest( "UNKNOWN", versions ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldSendModifierProtocolResponseForGivenProtocol()
    {
        // given
        Set<Integer> versions = asSet( TestModifierProtocols.allVersionsOf( COMPRESSION ) );
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), versions ) );

        // then
        ModifierProtocol expected = TestModifierProtocols.latest( COMPRESSION );
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( SUCCESS,  expected.identifier(), expected.version() ) );
    }

    @Test
    public void shouldNotCloseConnectionForGivenModifierProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), versions ) );

        // then
        assertUnfinished();
    }

    @Test
    public void shouldSendFailModifierProtocolResponseForUnknownVersion()
    {
        // given
        Set<Integer> versions = asSet( Integer.MAX_VALUE );
        server.handle( InitialMagicMessage.instance() );

        // when
        String protocolName = COMPRESSION.canonicalName();
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( FAILURE, protocolName, 0 ) );
    }

    @Test
    public void shouldNotCloseConnectionIfUnknownModifierProtocolVersion()
    {
        // given
        Set<Integer> versions = asSet( Integer.MAX_VALUE );
        server.handle( InitialMagicMessage.instance() );

        // when
        String protocolName = COMPRESSION.canonicalName();
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        assertUnfinished();
    }

    @Test
    public void shouldSendFailModifierProtocolResponseForUnknownProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( InitialMagicMessage.instance() );

        // when
        String protocolName = "let's just randomly reorder all the bytes";
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( FAILURE, protocolName, 0 ) );
    }

    @Test
    public void shouldNotCloseConnectionIfUnknownModifierProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( InitialMagicMessage.instance() );

        // when
        String protocolName = "let's just randomly reorder all the bytes";
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        assertUnfinished();
    }

    @Test
    public void shouldSendFailureOnUnknownProtocolSwitchOver()
    {
        // given
        int version = 1;
        String unknownProtocolName = "UNKNOWN";
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( unknownProtocolName, asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( unknownProtocolName, version, emptyList() ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackOnUnknownProtocolSwitchOver()
    {
        // given
        int version = 1;
        String unknownProtocolName = "UNKNOWN";
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( unknownProtocolName, asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( unknownProtocolName, version, emptyList() ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldSendFailureIfSwitchOverBeforeNegotiation()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), version, emptyList() ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackIfSwitchOverBeforeNegotiation()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), version, emptyList() ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldSendFailureIfSwitchOverDiffersFromNegotiatedProtocol()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), version + 1, emptyList() ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackIfSwitchOverDiffersFromNegotiatedProtocol()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), version + 1, emptyList() ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldSendFailureIfSwitchOverDiffersByNameFromNegotiatedModifierProtocol()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                asList( Pair.of( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), version) ) ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackIfSwitchOverDiffersByNameFromNegotiatedModifiedProtocol()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                asList( Pair.of(  ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), version) ) ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldSendFailureIfSwitchOverChangesOrderOfModifierProtocols()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                asList( Pair.of( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), version),
                        Pair.of( COMPRESSION.canonicalName(), version) ) ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackIfSwitchOverChangesOrderOfModifierProtocols()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                asList( Pair.of( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), version),
                        Pair.of( COMPRESSION.canonicalName(), version) ) ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldSendFailureIfSwitchOverDiffersByVersionFromNegotiatedModifierProtocol()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT_1.identifier(),
                version,
                asList( Pair.of( COMPRESSION.canonicalName(), version + 1 ) )
        ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackIfSwitchOverDiffersByVersionFromNegotiatedModifiedProtocol()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT_1.identifier(),
                version,
                asList( Pair.of( COMPRESSION.canonicalName(), version + 1 ) )
        ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldCompleteProtocolStackOnSuccessfulSwitchOverWithNoModifierProtocols()
    {
        // given
        int version = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), version, emptyList() ) );

        // then
        verify( channel ).writeAndFlush( InitialMagicMessage.instance() );
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        ProtocolStack protocolStack = server.protocolStackFuture().getNow( null );
        assertThat( protocolStack, equalTo( new ProtocolStack( RAFT_1, emptyList() ) ) );
    }

    @Test
    public void shouldCompleteProtocolStackOnSuccessfulSwitchOverWithModifierProtocols()
    {
        // given
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( RAFT_1.version()) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( SNAPPY.version() ) ) );
        server.handle( new ModifierProtocolRequest( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), asSet( ROT13.version() ) ) );

        // when
        List<Pair<String,Integer>> modifierRequest = asList(
                Pair.of( SNAPPY.identifier(), SNAPPY.version() ),
                Pair.of( ROT13.identifier(), ROT13.version() )
        );
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), RAFT_1.version(), modifierRequest ) );

        // then
        verify( channel ).writeAndFlush( InitialMagicMessage.instance() );
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        ProtocolStack protocolStack = server.protocolStackFuture().getNow( null );
        List<ModifierProtocol> modifiers = asList( SNAPPY, ROT13 );
        assertThat( protocolStack, equalTo( new ProtocolStack( RAFT_1, modifiers ) ) );
    }

    @Test
    public void shouldCompleteProtocolStackOnSuccessfulSwitchOverWithConfiguredModifierProtocols()
    {
        // given
        Set<Integer> requestedVersions = asSet( TestModifierProtocols.allVersionsOf( COMPRESSION ) );
        Integer expectedNegotiatedVersion = 1;
        List<Integer> configuredVersions = singletonList( expectedNegotiatedVersion );

        List<SupportedProtocols<ModifierProtocol>> supportedModifierProtocols =
                asList( new SupportedProtocols<>( COMPRESSION, configuredVersions ) );

        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestModifierProtocols.values(), supportedModifierProtocols );

        HandshakeServer server = new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( RAFT_1.version()) ) );
        server.handle( new ModifierProtocolRequest(
                COMPRESSION.canonicalName(),
                requestedVersions ) );

        // when
        List<Pair<String,Integer>> modifierRequest = asList( Pair.of( SNAPPY.identifier(), SNAPPY.version() ) );
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), RAFT_1.version(), modifierRequest ) );

        // then
        verify( channel ).writeAndFlush( InitialMagicMessage.instance() );
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        ProtocolStack protocolStack = server.protocolStackFuture().getNow( null );
        List<ModifierProtocol> modifiers = asList( SNAPPY );
        assertThat( protocolStack, equalTo( new ProtocolStack( RAFT_1, modifiers ) ) );
    }

    @Test
    public void shouldSuccessfullySwitchOverWhenServerHasConfiguredRaftVersions()
    {
        // given
        Set<Integer> requestedVersions = asSet( TestApplicationProtocols.allVersionsOf( RAFT ) );
        Integer expectedNegotiatedVersion = 1;
        ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository(
                TestApplicationProtocols.values(), new SupportedProtocols<>( RAFT, singletonList( expectedNegotiatedVersion ) ) );

        HandshakeServer server = new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), requestedVersions ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), expectedNegotiatedVersion, emptyList() ) );

        // then
        verify( channel ).writeAndFlush( InitialMagicMessage.instance() );
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        ProtocolStack protocolStack = server.protocolStackFuture().getNow( null );
        ProtocolStack expectedProtocolStack = new ProtocolStack(
                applicationProtocolRepository.select( RAFT.canonicalName(), expectedNegotiatedVersion ).get(),
                emptyList() );
        assertThat( protocolStack, equalTo( expectedProtocolStack ) );
    }

    private void assertUnfinished()
    {
        verify( channel, never() ).dispose();
        assertFalse( server.protocolStackFuture().isDone() );
    }

    private void assertExceptionallyCompletedProtocolStackFuture()
    {
        assertTrue( server.protocolStackFuture().isCompletedExceptionally() );
        try
        {
            server.protocolStackFuture().getNow( null );
        }
        catch ( CompletionException ex )
        {
            assertThat( ex.getCause(), Matchers.instanceOf( ServerHandshakeException.class ) );
        }
    }
}
