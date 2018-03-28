package org.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.RequestMessageType;

import static org.junit.Assert.assertEquals;

public class CatchUpRequestTest
{
    @Test
    public void shouldDeserializeAndSerialize() throws IOException
    {
        // given
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.heapBuffer();

        // when
        String id = "this id #";
        new SimpleCatchupRequest( id ).encodeMessage( new NetworkFlushableChannelNetty4( byteBuf ) );
        ByteBuf copy = byteBuf.copy();
        String decodeMessage1 = SimpleCatchupRequest.decodeMessage( copy );
        String decodeMessage2 = SimpleCatchupRequest.decodeMessage( new NetworkReadableClosableChannelNetty4( byteBuf ) );

        // then
        assertEquals( id, decodeMessage1 );
        assertEquals( id, decodeMessage2 );
    }

    private class SimpleCatchupRequest extends CatchUpRequest
    {
        private SimpleCatchupRequest( String id )
        {
            super( id );
        }

        @Override
        public RequestMessageType messageType()
        {
            return RequestMessageType.CORE_SNAPSHOT;
        }
    }
}