/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Map;

import org.neo4j.bolt.v1.messaging.BoltRequestMessageHandler;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.MapValue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.bolt.v1.messaging.message.AckFailureMessage.ackFailure;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.ResetMessage.reset;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;

@RunWith( Parameterized.class )
public class MessageDecoderTest
{
    private EmbeddedChannel channel;

    @Parameterized.Parameter( 0 )
    public Neo4jPack pack;

    @Parameterized.Parameter( 1 )
    public String name;

    @Parameterized.Parameters( name = "{1}" )
    public static Object[][] testParameters()
    {
        return new Object[][]{new Object[]{new Neo4jPackV1(), "V1"}, new Object[]{new Neo4jPackV2(), "V2"}};
    }

    @After
    public void cleanup()
    {
        if ( channel != null )
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldDispatchInit() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( new MessageDecoder( pack, handler ) );

        String userAgent = "Test/User Agent 1.0";
        Map<String, Object> authToken = MapUtil.map( "scheme", "basic", "principal", "user", "credentials", "password" );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( pack, init( userAgent, authToken ) ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onInit( userAgent, authToken );
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchAckFailure() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( new MessageDecoder( pack, handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( pack, ackFailure() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onAckFailure();
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchReset() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( new MessageDecoder( pack, handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( pack, reset() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onReset();
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchRun() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( new MessageDecoder( pack, handler ) );

        String statement = "RETURN 1";
        MapValue parameters = ValueUtils.asMapValue( MapUtil.map( "param1", 1, "param2", "2", "param3", true, "param4", 5.0 ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( pack, run( statement, parameters ) ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onRun( statement, parameters );
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchDiscardAll() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( new MessageDecoder( pack, handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( pack, discardAll() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onDiscardAll();
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchPullAll() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( new MessageDecoder( pack, handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( pack, pullAll() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onPullAll();
        verifyNoMoreInteractions( handler );
    }

}
