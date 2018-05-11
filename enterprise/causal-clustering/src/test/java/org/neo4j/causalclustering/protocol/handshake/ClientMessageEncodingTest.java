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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@RunWith( Parameterized.class )
public class ClientMessageEncodingTest
{
    private final ClientMessage message;
    private final ServerMessageEncoder encoder = new ServerMessageEncoder();
    private final ClientMessageDecoder decoder = new ClientMessageDecoder();

    private List<Object> encodeDecode( ClientMessage message ) throws ClientHandshakeException
    {
        ByteBuf byteBuf = Unpooled.directBuffer();
        List<Object> output = new ArrayList<>();

        encoder.encode( null, message, byteBuf );
        decoder.decode( null, byteBuf, output );

        return output;
    }

    @Parameterized.Parameters( name = "ResponseMessage-{0}" )
    public static Collection<ClientMessage> data()
    {
        return Arrays.asList(
                new ApplicationProtocolResponse( StatusCode.FAILURE, "protocol", 13 ),
                new ModifierProtocolResponse( StatusCode.SUCCESS, "modifier", "7" ),
                new SwitchOverResponse( StatusCode.FAILURE )
                );
    }

    public ClientMessageEncodingTest( ClientMessage message )
    {
        this.message = message;
    }

    @Test
    public void shouldCompleteEncodingRoundTrip() throws ClientHandshakeException
    {
        //when
        List<Object> output = encodeDecode( message );

        //then
        Assert.assertThat( output, hasSize( 1 ) );
        Assert.assertThat( output.get( 0 ), equalTo( message ) );
    }
}
