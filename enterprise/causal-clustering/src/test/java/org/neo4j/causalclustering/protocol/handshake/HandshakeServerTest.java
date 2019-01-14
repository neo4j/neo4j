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
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.GRATUITOUS_OBFUSCATION;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.FAILURE;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZ4;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZO;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.ROT13;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * @see HandshakeServerEnsureMagicTest
 */
public class HandshakeServerTest
{
    private Channel channel = mock( Channel.class );
    private ApplicationSupportedProtocols supportedApplicationProtocol =
            new ApplicationSupportedProtocols( RAFT, emptyList() );
    private Collection<ModifierSupportedProtocols> supportedModifierProtocols = asList(
            new ModifierSupportedProtocols( COMPRESSION, TestModifierProtocols.listVersionsOf( COMPRESSION ) ),
            new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, TestModifierProtocols.listVersionsOf( GRATUITOUS_OBFUSCATION ) )
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
                new ApplicationProtocolRequest( TestApplicationProtocols.CATCHUP_1.category(), asSet( TestApplicationProtocols.CATCHUP_1.implementation() ) ) );

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
                new ApplicationProtocolRequest( TestApplicationProtocols.CATCHUP_1.category(), asSet( TestApplicationProtocols.CATCHUP_1.implementation() ) ) );

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
                new ApplicationProtocolResponse( SUCCESS, TestApplicationProtocols.RAFT_3.category(), TestApplicationProtocols.RAFT_3.implementation() ) );
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
        Set<String> versions = asSet( TestModifierProtocols.allVersionsOf( COMPRESSION ) );
        server.handle( InitialMagicMessage.instance() );

        // when
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), versions ) );

        // then
        ModifierProtocol expected = TestModifierProtocols.latest( COMPRESSION );
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( SUCCESS,  expected.category(), expected.implementation() ) );
    }

    @Test
    public void shouldNotCloseConnectionForGivenModifierProtocol()
    {
        // given
        Set<String> versions = asSet( SNAPPY.implementation(), LZO.implementation(), LZ4.implementation() );
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
        Set<String> versions = asSet( "Not a real protocol" );
        server.handle( InitialMagicMessage.instance() );

        // when
        String protocolName = COMPRESSION.canonicalName();
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( FAILURE, protocolName, "" ) );
    }

    @Test
    public void shouldNotCloseConnectionIfUnknownModifierProtocolVersion()
    {
        // given
        Set<String> versions = asSet( "not a real algorithm" );
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
        Set<String> versions = asSet( SNAPPY.implementation(), LZO.implementation(), LZ4.implementation() );
        server.handle( InitialMagicMessage.instance() );

        // when
        String protocolName = "let's just randomly reorder all the bytes";
        server.handle( new ModifierProtocolRequest( protocolName, versions ) );

        // then
        verify( channel ).writeAndFlush(
                new ModifierProtocolResponse( FAILURE, protocolName, "" ) );
    }

    @Test
    public void shouldNotCloseConnectionIfUnknownModifierProtocol()
    {
        // given
        Set<String> versions = asSet( SNAPPY.implementation(), LZO.implementation(), LZ4.implementation() );
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
        server.handle( new SwitchOverRequest( RAFT_1.category(), version, emptyList() ) );

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
        server.handle( new SwitchOverRequest( RAFT_1.category(), version, emptyList() ) );

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
        server.handle( new SwitchOverRequest( RAFT_1.category(), version + 1, emptyList() ) );

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
        server.handle( new SwitchOverRequest( RAFT_1.category(), version + 1, emptyList() ) );

        // then
        assertExceptionallyCompletedProtocolStackFuture();
    }

    @Test
    public void shouldSendFailureIfSwitchOverDiffersByNameFromNegotiatedModifierProtocol()
    {
        // given
        String modifierVersion = ROT13.implementation();
        int applicationVersion = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( applicationVersion ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( modifierVersion ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(), applicationVersion,
                asList( Pair.of( GRATUITOUS_OBFUSCATION.canonicalName(), modifierVersion ) ) ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackIfSwitchOverDiffersByNameFromNegotiatedModifiedProtocol()
    {
        // given
        String modifierVersion = ROT13.implementation();
        int applicationVersion = 1;
        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( applicationVersion ) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( modifierVersion ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                applicationVersion,
                asList( Pair.of(  GRATUITOUS_OBFUSCATION.canonicalName(), modifierVersion ) ) ) );

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
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( SNAPPY.implementation() ) ) );
        server.handle( new ModifierProtocolRequest( GRATUITOUS_OBFUSCATION.canonicalName(), asSet( ROT13.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                asList( Pair.of( GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ),
                        Pair.of( COMPRESSION.canonicalName(), SNAPPY.implementation() ) ) ) );

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
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( SNAPPY.implementation() ) ) );
        server.handle( new ModifierProtocolRequest( GRATUITOUS_OBFUSCATION.canonicalName(), asSet( ROT13.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT.canonicalName(),
                version,
                asList( Pair.of( GRATUITOUS_OBFUSCATION.canonicalName(), ROT13.implementation() ),
                        Pair.of( COMPRESSION.canonicalName(), SNAPPY.implementation() ) ) ) );

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
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( SNAPPY.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT_1.category(),
                version,
                asList( Pair.of( COMPRESSION.canonicalName(), LZ4.implementation() ) )
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
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( SNAPPY.implementation() ) ) );

        // when
        server.handle( new SwitchOverRequest(
                RAFT_1.category(),
                version,
                asList( Pair.of( COMPRESSION.canonicalName(), LZ4.implementation() ) )
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
        server.handle( new SwitchOverRequest( RAFT_1.category(), version, emptyList() ) );

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
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( RAFT_1.implementation()) ) );
        server.handle( new ModifierProtocolRequest( COMPRESSION.canonicalName(), asSet( SNAPPY.implementation() ) ) );
        server.handle( new ModifierProtocolRequest( GRATUITOUS_OBFUSCATION.canonicalName(), asSet( ROT13.implementation() ) ) );

        // when
        List<Pair<String,String>> modifierRequest = asList(
                Pair.of( SNAPPY.category(), SNAPPY.implementation() ),
                Pair.of( ROT13.category(), ROT13.implementation() )
        );
        server.handle( new SwitchOverRequest( RAFT_1.category(), RAFT_1.implementation(), modifierRequest ) );

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
        Set<String> requestedVersions = asSet( TestModifierProtocols.allVersionsOf( COMPRESSION ) );
        String expectedNegotiatedVersion = SNAPPY.implementation();
        List<String> configuredVersions = singletonList( expectedNegotiatedVersion );

        List<ModifierSupportedProtocols> supportedModifierProtocols =
                asList( new ModifierSupportedProtocols( COMPRESSION, configuredVersions ) );

        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestModifierProtocols.values(), supportedModifierProtocols );

        HandshakeServer server = new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( RAFT_1.implementation()) ) );
        server.handle( new ModifierProtocolRequest(
                COMPRESSION.canonicalName(),
                requestedVersions ) );

        // when
        List<Pair<String,String>> modifierRequest = asList( Pair.of( SNAPPY.category(), SNAPPY.implementation() ) );
        server.handle( new SwitchOverRequest( RAFT_1.category(), RAFT_1.implementation(), modifierRequest ) );

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
                TestApplicationProtocols.values(), new ApplicationSupportedProtocols( RAFT, singletonList( expectedNegotiatedVersion ) ) );

        HandshakeServer server = new HandshakeServer( applicationProtocolRepository, modifierProtocolRepository, channel );

        server.handle( InitialMagicMessage.instance() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), requestedVersions ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.category(), expectedNegotiatedVersion, emptyList() ) );

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
