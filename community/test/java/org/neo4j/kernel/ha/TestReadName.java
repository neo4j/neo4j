package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;

import java.nio.ByteOrder;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

public class TestReadName
{
    @Test
    public void canReadLittleEndianString() throws Exception
    {
        test( ByteOrder.LITTLE_ENDIAN );
    }

    @Test
    public void canReadBigEndianString() throws Exception
    {
        test( ByteOrder.BIG_ENDIAN );
    }

    private void test( ByteOrder endianness )
    {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer( endianness, 23 );
        ChunkedResponse.writeName( "hello world".toCharArray(), buffer );
        assertEquals( "hello world", readString( buffer ) );
    }

    private static String readString( ChannelBuffer buffer )
    { // NOTE: Some copied/pasted logic...
        int length = buffer.readUnsignedByte() * 2;
        byte[] data = new byte[length];
        buffer.readBytes( data, 0, length );
        return InputReader.makeString( data, buffer.order() );
    }
}
