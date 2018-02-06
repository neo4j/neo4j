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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.neo4j.helpers.collection.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolIdentifier;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * @see HandshakeClientEnsureMagicTest
 */
public class HandshakeClientTest
{
    private HandshakeClient client = new HandshakeClient();
    private Channel channel = mock( Channel.class );
    private ProtocolRepository<ApplicationProtocol> applicationProtocolRepository = new ProtocolRepository<>( TestApplicationProtocols.values() );
    private ProtocolRepository<ModifierProtocol> modifierProtocolRepository = new ProtocolRepository<>( TestModifierProtocols.values() );
    private ApplicationProtocolIdentifier applicationProtocolIdentifier = ApplicationProtocolIdentifier.RAFT;
    private int raftVersion = TestApplicationProtocols.latest( ApplicationProtocolIdentifier.RAFT ).version();
    private ApplicationProtocol expectedApplicationProtocol =
            applicationProtocolRepository.select( applicationProtocolIdentifier.canonicalName(), raftVersion ).get();

    @Test
    public void shouldSendInitialMagicOnInitiation()
    {
        client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );

        verify( channel ).write( InitialMagicMessage.instance() );
    }

    @Test
    public void shouldSendApplicationProtocolRequestOnInitiation()
    {
        client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );

        ApplicationProtocolRequest expectedMessage = new ApplicationProtocolRequest(
                applicationProtocolIdentifier.canonicalName(),
                applicationProtocolRepository.getAll( applicationProtocolIdentifier ).versions()
        );

        verify( channel ).writeAndFlush( expectedMessage );
    }

    @Test
    public void shouldSendModifierProtocolRequestsOnInitiation()
    {
        // when
        client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository,
                asSet( ModifierProtocolIdentifier.values() ) );

        // then
        Stream.of( ModifierProtocolIdentifier.values() ).forEach( modifierProtocolIdentifier ->
                {
                    Set<Integer> versions = modifierProtocolRepository.getAll( modifierProtocolIdentifier ).versions();
                    verify( channel ).write( new ModifierProtocolRequest( modifierProtocolIdentifier.canonicalName(), versions ) );
                } );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackOnReceivingIncorrectMagic()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );

        // when
        client.handle( new InitialMagicMessage( "totally legit" ) );

        // then
        assertCompletedExceptionally( protocolStackCompletableFuture );
    }

    @Test
    public void shouldAcceptCorrectMagic()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );

        // when
        client.handle( InitialMagicMessage.instance() );

        // then
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseNotSuccessful()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.FAILURE, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // then
        assertCompletedExceptionally( protocolStackCompletableFuture );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseForIncorrectProtocol()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, "zab", raftVersion ) );

        // then
        assertCompletedExceptionally( protocolStackCompletableFuture );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenApplicationProtocolResponseForUnsupportedVersion()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), Integer.MAX_VALUE ) );

        // then
        assertCompletedExceptionally( protocolStackCompletableFuture );
    }

    @Test
    public void shouldSendSwitchOverRequestIfNoModifierProtocolsToRequestOnApplicationProtocolResponse()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // then
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, emptyList() ) );
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldNotSendSwitchOverRequestOnModifierProtocolResponseIfNotAllModifierProtocolResponsesReceived()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository,
                        asSet( ModifierProtocolIdentifier.COMPRESSION, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION ) );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );

        // then
        verify( channel, never() ).writeAndFlush( any( SwitchOverRequest.class ) );
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldNotSendSwitchOverRequestIfApplicationProtocolResponseNotReceivedOnModifierProtocolResponseReceive()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository,
                        asSet( ModifierProtocolIdentifier.COMPRESSION ) );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );

        // then
        verify( channel, never() ).writeAndFlush( any( SwitchOverRequest.class ) );
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldSendSwitchOverRequestOnModifierProtocolResponseIfAllModifierProtocolResponsesReceived()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository,
                        asSet( ModifierProtocolIdentifier.COMPRESSION, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION ) );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), 1 ) );

        // then
        List<Pair<String,Integer>> switchOverModifierProtocols = asList( Pair.of( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), 1 ),
                Pair.of( ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldNotIncludeModifierProtocolInSwitchOverRequestIfNotSuccessful()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository,
                        asSet( ModifierProtocolIdentifier.COMPRESSION, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION ) );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        client.handle( new ModifierProtocolResponse( StatusCode.FAILURE, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), 1 ) );

        // then
        List<Pair<String,Integer>> switchOverModifierProtocols = asList( Pair.of( ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldNotIncludeModifierProtocolInSwitchOverRequestIfUnsupportedProtocol()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository,
                        asSet( ModifierProtocolIdentifier.COMPRESSION, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION ) );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        client.handle( new ModifierProtocolResponse( StatusCode.SUCCESS, "not a protocol", 1 ) );

        // then
        List<Pair<String,Integer>> switchOverModifierProtocols = asList( Pair.of( ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldNotIncludeModifierProtocolInSwitchOverRequestIfUnsupportedVersion()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository,
                        asSet( ModifierProtocolIdentifier.COMPRESSION, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION ) );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle(
                new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        client.handle(
                new ModifierProtocolResponse( StatusCode.SUCCESS, ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION.canonicalName(), Integer.MAX_VALUE ) );

        // then
        List<Pair<String,Integer>> switchOverModifierProtocols = asList( Pair.of( ModifierProtocolIdentifier.COMPRESSION.canonicalName(), 1 ) );
        verify( channel ).writeAndFlush( new SwitchOverRequest( applicationProtocolIdentifier.canonicalName(), raftVersion, switchOverModifierProtocols ) );
        assertFalse( protocolStackCompletableFuture.isDone() );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenSwitchOverResponseNotSuccess()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );

        // when
        client.handle( new SwitchOverResponse( StatusCode.FAILURE ) );

        // then
        assertCompletedExceptionally( protocolStackCompletableFuture );
    }

    @Test
    public void shouldExceptionallyCompleteProtocolStackWhenProtocolStackNotSet()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier, modifierProtocolRepository, emptySet() );
        client.handle( InitialMagicMessage.instance() );

        // when
        client.handle( new SwitchOverResponse( StatusCode.SUCCESS ) );

        // then
        assertCompletedExceptionally( protocolStackCompletableFuture );
    }

    @Test
    public void shouldCompleteProtocolStackOnSwitchoverResponse()
    {
        // given
        CompletableFuture<ProtocolStack> protocolStackCompletableFuture =
                client.initiate( channel, applicationProtocolRepository, applicationProtocolIdentifier,
                        modifierProtocolRepository, asSet( ModifierProtocolIdentifier.COMPRESSION ) );
        client.handle( InitialMagicMessage.instance() );
        client.handle( new ApplicationProtocolResponse( StatusCode.SUCCESS, applicationProtocolIdentifier.canonicalName(), raftVersion ) );
        client.handle(
                new ModifierProtocolResponse( StatusCode.SUCCESS, TestModifierProtocols.SNAPPY.identifier(), TestModifierProtocols.SNAPPY.version() ) );

        // when
        client.handle( new SwitchOverResponse( StatusCode.SUCCESS ) );

        // then
        ProtocolStack protocolStack = protocolStackCompletableFuture.getNow( null );
        assertThat( protocolStack, equalTo( new ProtocolStack( expectedApplicationProtocol, singletonList( TestModifierProtocols.SNAPPY ) ) ) );
    }

    private void assertCompletedExceptionally( CompletableFuture<ProtocolStack> protocolStackCompletableFuture )
    {
        assertTrue( protocolStackCompletableFuture.isCompletedExceptionally() );
        try
        {
            protocolStackCompletableFuture.getNow( null );
        }
        catch ( CompletionException ex )
        {
            assertThat( ex.getCause(), instanceOf( ClientHandshakeException.class ) );
        }
    }
}
