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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.collection.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith( Parameterized.class )
public class ServerMessageEncodingTest
{
    private final ServerMessage message;
    private final ClientMessageEncoder encoder = new ClientMessageEncoder();
    private final ServerMessageDecoder decoder = new ServerMessageDecoder();

    private List<Object> encodeDecode( ServerMessage message )
    {
        ByteBuf byteBuf = Unpooled.directBuffer();
        List<Object> output = new ArrayList<>();

        encoder.encode( null, message, byteBuf );
        decoder.decode( null, byteBuf, output );

        return output;
    }

    @Parameterized.Parameters( name = "ResponseMessage-{0}" )
    public static Collection<ServerMessage> data()
    {
        return asList(
                new ApplicationProtocolRequest( "protocol", asSet( 3,7,13 ) ),
                new InitialMagicMessage( "Magic string" ),
                new ModifierProtocolRequest( "modifierProtocol", asSet( "Foo", "Bar", "Baz" ) ),
                new SwitchOverRequest( "protocol", 38, emptyList() ),
                new SwitchOverRequest( "protocol", 38,
                        asList( Pair.of( "mod1", "Foo" ), Pair.of( "mod2" , "Quux" ) ) )
                );
    }

    public ServerMessageEncodingTest( ServerMessage message )
    {
        this.message = message;
    }

    @Test
    public void shouldCompleteEncodingRoundTrip()
    {
        //when
        List<Object> output = encodeDecode( message );

        //then
        Assert.assertThat( output, hasSize( 1 ) );
        Assert.assertThat( output.get( 0 ), equalTo( message ) );
    }
}
