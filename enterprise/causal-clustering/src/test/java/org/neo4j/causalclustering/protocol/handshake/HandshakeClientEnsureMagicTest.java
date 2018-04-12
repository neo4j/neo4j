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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZ4;

@RunWith( Parameterized.class )
public class HandshakeClientEnsureMagicTest
{
    private CompletableFuture<ProtocolStack> protocolStackCompletableFuture;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<ClientMessage> data()
    {
        return asList(
                new ApplicationProtocolResponse( StatusCode.SUCCESS, "protocol", RAFT_1.implementation() ),
                new ModifierProtocolResponse( StatusCode.SUCCESS, "modifier", LZ4.implementation() ),
                new SwitchOverResponse( StatusCode.SUCCESS )
        );
    }

    @Parameterized.Parameter
    public ClientMessage message;

    private Channel channel = mock( Channel.class );

    private ApplicationSupportedProtocols supportedApplicationProtocol =
            new ApplicationSupportedProtocols( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) );
    private ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedApplicationProtocol );
    private ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), emptyList() );

    private HandshakeClient client = new HandshakeClient();

    @Before
    public void setUp()
    {
        protocolStackCompletableFuture = client.initiate( channel, applicationProtocolRepository, modifierProtocolRepository );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfMagicHasNotBeenSent()
    {
        message.dispatch( client );
    }

    @Test( expected = ClientHandshakeException.class )
    public void shouldCompleteExceptionallyIfMagicHasNotBeenSent() throws Throwable
    {
        try
        {
            message.dispatch( client );
        }
        catch ( Exception ignored )
        {
            // swallow
        }

        try
        {
            protocolStackCompletableFuture.getNow( null );
        }
        catch ( CompletionException ex )
        {
            throw ex.getCause();
        }
    }

    @Test
    public void shouldNotThrowIfMagicHasBeenSent()
    {
        // given
        InitialMagicMessage.instance().dispatch( client );

        // when
        message.dispatch( client );

        // then pass
    }

    @Test
    public void shouldNotCompleteExceptionallyIfMagicHasBeenSent()
    {
        // given
        InitialMagicMessage.instance().dispatch( client );

        // when
        message.dispatch( client );

        // then future should either not complete exceptionally or do so for non-magic reasons
        try
        {
            protocolStackCompletableFuture.getNow( null );
        }
        catch ( CompletionException ex )
        {
            assertThat( ex.getMessage().toLowerCase(), not( containsString( "magic" ) ) );
        }
    }
}
