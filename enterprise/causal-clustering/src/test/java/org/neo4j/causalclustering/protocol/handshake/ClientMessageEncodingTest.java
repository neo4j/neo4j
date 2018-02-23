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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClientMessageEncodingTest
{
    private final ServerMessageEncoder encoder = new ServerMessageEncoder();
    private final ClientMessageDecoder decoder = new ClientMessageDecoder();

    private List<Object> encodeDecode( ClientMessage message )
    {
        ByteBuf byteBuf = Unpooled.directBuffer();
        List<Object> output = new ArrayList<>();

        encoder.encode( null, message, byteBuf );
        decoder.decode( null, byteBuf, output );

        return output;
    }

    public static Collection<ClientMessage> data()
    {
        return Arrays.asList(
                new ApplicationProtocolResponse( StatusCode.FAILURE, "protocol", 13 ),
                new ModifierProtocolResponse(),
                new SwitchOverResponse( StatusCode.FAILURE )
                );
    }

    @ParameterizedTest
    @MethodSource( value = "data" )
    public void shouldCompleteEncodingRoundTrip( ClientMessage message ) throws Throwable
    {
        //when
        List<Object> output = encodeDecode( message );

        //then
        Assert.assertThat( output, hasSize( 1 ) );
        Assert.assertThat( output.get( 0 ), equalTo( message ) );
    }
}
