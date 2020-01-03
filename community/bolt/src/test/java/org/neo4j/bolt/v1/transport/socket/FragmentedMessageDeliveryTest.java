/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.transport.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.SynchronousBoltConnection;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.messaging.RecordingByteChannel;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.logging.internal.NullLogService;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

/**
 * This tests network fragmentation of messages. Given a set of messages, it will serialize and chunk the message up
 * to a specified chunk size. Then it will split that data into a specified number of fragments, trying every possible
 * permutation of fragment sizes for the specified number. For instance, assuming an unfragmented message size of 15,
 * and a fragment count of 3, it will create fragment size permutations like:
 * <p/>
 * [1,1,13]
 * [1,2,12]
 * [1,3,11]
 * ..
 * [12,1,1]
 * <p/>
 * For each permutation, it delivers the fragments to the protocol implementation, and asserts the protocol handled
 * them properly.
 */
@RunWith( Parameterized.class )
public class FragmentedMessageDeliveryTest
{
    private EmbeddedChannel channel;
    // Only test one chunk size for now, this can be parameterized to test lots of different ones
    private int chunkSize = 16;

    // Only test one message for now. This can be parameterized later to test lots of different ones
    private RequestMessage[] messages = new RequestMessage[]{new RunMessage( "Mjölnir" )};

    @Parameter
    public Neo4jPack neo4jPack;

    @Parameters( name = "{0}" )
    public static List<Neo4jPack> parameters()
    {
        return Arrays.asList( new Neo4jPackV1(), new Neo4jPackV2() );
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
    public void testFragmentedMessageDelivery() throws Throwable
    {
        // Given
        byte[] unfragmented = serialize( chunkSize, messages );

        // When & Then
        int n = unfragmented.length;
        for ( int i = 1; i < n - 1; i++ )
        {
            for ( int j = 1; j < n - i; j++ )
            {
                testPermutation( unfragmented, i, j, n - i - j );
            }
        }
    }

    private void testPermutation( byte[] unfragmented, int... sizes ) throws Exception
    {
        int pos = 0;
        ByteBuf[] fragments = new ByteBuf[sizes.length];
        for ( int i = 0; i < sizes.length; i++ )
        {
            fragments[i] = wrappedBuffer( unfragmented, pos, sizes[i] );
            pos += sizes[i];
        }
        testPermutation( unfragmented, fragments );
    }

    private void testPermutation( byte[] unfragmented, ByteBuf[] fragments ) throws Exception
    {
        // Given
        channel = new EmbeddedChannel();
        BoltChannel boltChannel = newBoltChannel( channel );

        BoltStateMachine machine = mock( BoltStateMachine.class );
        SynchronousBoltConnection boltConnection = new SynchronousBoltConnection( machine );
        NullLogService logging = NullLogService.getInstance();
        BoltProtocol boltProtocol = new BoltProtocolV1( boltChannel, ( ch, s ) -> boltConnection, ( v, ch ) -> machine, logging );
        boltProtocol.install();

        // When data arrives split up according to the current permutation
        for ( ByteBuf fragment : fragments )
        {
            channel.writeInbound( fragment.readerIndex( 0 ).retain() );
        }

        // Then the session should've received the specified messages, and the protocol should be in a nice clean state
        try
        {
            RequestMessage run = new RunMessage( "Mjölnir", EMPTY_MAP );
            verify( machine ).process( eq( run ), any( BoltResponseHandler.class ) );
        }
        catch ( AssertionError e )
        {
            throw new AssertionError( "Failed to handle fragmented delivery.\n" +
                                      "Messages: " + Arrays.toString( messages ) + "\n" +
                                      "Chunk size: " + chunkSize + "\n" +
                                      "Serialized data delivered in fragments: " + describeFragments( fragments ) +
                                      "\n" +
                                      "Unfragmented data: " + HexPrinter.hex( unfragmented ) + "\n", e );
        }
    }

    private String describeFragments( ByteBuf[] fragments )
    {
        StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < fragments.length; i++ )
        {
            if ( i > 0 )
            {
                sb.append( "," );
            }
            sb.append( fragments[i].capacity() );
        }
        return sb.toString();
    }

    private byte[] serialize( int chunkSize, RequestMessage... msgs ) throws IOException
    {
        byte[][] serialized = new byte[msgs.length][];
        for ( int i = 0; i < msgs.length; i++ )
        {
            RecordingByteChannel channel = new RecordingByteChannel();

            BoltRequestMessageWriter writer = new BoltRequestMessageWriter(
                    new Neo4jPackV1().newPacker( new BufferedChannelOutput( channel ) ) );
            writer.write( msgs[i] ).flush();
            serialized[i] = channel.getBytes();
        }
        return Chunker.chunk( chunkSize, serialized );
    }

    private static BoltChannel newBoltChannel( EmbeddedChannel rawChannel )
    {
        BoltChannel boltChannel = mock( BoltChannel.class );
        when( boltChannel.rawChannel() ).thenReturn( rawChannel );
        return boltChannel;
    }
}
