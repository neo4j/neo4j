package org.neo4j.kernel.ha;

import static org.junit.Assert.assertTrue;

import java.nio.ByteOrder;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

public class TestReadInteger extends InputReader
{
    private static final long EXPECTED = 0x0123456789ABCDEFL;
    enum Size
    {
        BYTE( 1 ),
        SHORT( 2 ),
        MEDIUM( 3 ),
        INT( 4 ),
        LONG( 8 );
        private final int size;
        private final long expected;

        private Size( int size )
        {
            this.size = size;
            this.expected = EXPECTED & mask( size );
        }

        private static long mask( int size )
        {
            long result = 0;
            for ( int i = size; i > 0; --i )
            {
                result <<= 8;
                result |= 0xFF;
            }
            return result;
        }
    }

    // Single Buffer

    @Test
    public void canReadByteLittleEndian() throws Exception
    {
        test( Size.BYTE, ByteOrder.LITTLE_ENDIAN, 1 );
    }

    @Test
    public void canReadByteBigEndian() throws Exception
    {
        test( Size.BYTE, ByteOrder.BIG_ENDIAN, 1 );
    }

    @Test
    public void canReadShortLittleEndian() throws Exception
    {
        test( Size.SHORT, ByteOrder.LITTLE_ENDIAN, 1 );
    }

    @Test
    public void canReadShortBigEndian() throws Exception
    {
        test( Size.SHORT, ByteOrder.BIG_ENDIAN, 1 );
    }

    @Test
    public void canReadMediumLittleEndian() throws Exception
    {
        test( Size.MEDIUM, ByteOrder.LITTLE_ENDIAN, 1 );
    }

    @Test
    public void canReadMediumBigEndian() throws Exception
    {
        test( Size.MEDIUM, ByteOrder.BIG_ENDIAN, 1 );
    }

    @Test
    public void canReadIntLittleEndian() throws Exception
    {
        test( Size.INT, ByteOrder.LITTLE_ENDIAN, 1 );
    }

    @Test
    public void canReadIntBigEndian() throws Exception
    {
        test( Size.INT, ByteOrder.BIG_ENDIAN, 1 );
    }

    @Test
    public void canReadLongLittleEndian() throws Exception
    {
        test( Size.LONG, ByteOrder.LITTLE_ENDIAN, 1 );
    }

    @Test
    public void canReadLongBigEndian() throws Exception
    {
        test( Size.LONG, ByteOrder.BIG_ENDIAN, 1 );
    }

    // Two Buffers

    @Test
    public void canReadSplitShortLittleEndian() throws Exception
    {
        test( Size.SHORT, ByteOrder.LITTLE_ENDIAN, 2 );
    }

    @Test
    public void canReadSplitShortBigEndian() throws Exception
    {
        test( Size.SHORT, ByteOrder.BIG_ENDIAN, 2 );
    }

    @Test
    public void canReadSplitMediumLittleEndian() throws Exception
    {
        test( Size.MEDIUM, ByteOrder.LITTLE_ENDIAN, 2 );
    }

    @Test
    public void canReadSplitMediumBigEndian() throws Exception
    {
        test( Size.MEDIUM, ByteOrder.BIG_ENDIAN, 2 );
    }

    @Test
    public void canReadSplitIntLittleEndian() throws Exception
    {
        test( Size.INT, ByteOrder.LITTLE_ENDIAN, 2 );
    }

    @Test
    public void canReadSplitIntBigEndian() throws Exception
    {
        test( Size.INT, ByteOrder.BIG_ENDIAN, 2 );
    }

    @Test
    public void canReadSplitLongLittleEndian() throws Exception
    {
        test( Size.LONG, ByteOrder.LITTLE_ENDIAN, 2 );
    }

    @Test
    public void canReadSplitLongBigEndian() throws Exception
    {
        test( Size.LONG, ByteOrder.BIG_ENDIAN, 2 );
    }

    // HELPER METHODS

    private ChannelBuffer[] split( ChannelBuffer buffer, int times )
    {
        final int chunk = buffer.readableBytes() / times;
        ChannelBuffer[] result = new ChannelBuffer[times];
        for ( int i = 0; i < result.length; i++ )
        {
            if ( i == result.length - 1 )
            {
                result[i] = ChannelBuffers.buffer( buffer.order(), buffer.readableBytes() );
                buffer.readBytes( result[i], buffer.readableBytes() );
            }
            else
            {
                result[i] = ChannelBuffers.buffer( buffer.order(), chunk );
                buffer.readBytes( result[i], chunk );
            }
        }
        return result;
    }

    private void test( Size size, ByteOrder endianness, int split )
    {
        ChannelBuffer buffer = ChannelBuffers.buffer( endianness, size.size );
        switch ( size )
        {
        case BYTE:
            buffer.writeByte( (int) size.expected );
            break;
        case SHORT:
            buffer.writeShort( (int) size.expected );
            break;
        case MEDIUM:
            buffer.writeMedium( (int) size.expected );
            break;
        case INT:
            buffer.writeInt( (int) size.expected );
            break;
        case LONG:
            buffer.writeLong( size.expected );
            break;
        }
        assertRead( new ExpectInteger( size ), split( buffer, split ) );
    }

    private void assertRead( ExpectInteger reader, ChannelBuffer... buffers )
    {
        for ( int i = 0; ( !reader.isDone ) && ( i < buffers.length ); i++ )
        {
            System.out.println( i + 1 );
            reader.read( buffers[i] );
        }
        assertTrue( "full integer not read", reader.isDone );
    }

    private final class ExpectInteger extends ReadInteger
    {
        boolean isDone = false;
        private final long expected;

        ExpectInteger( Size size )
        {
            super( size.size );
            this.expected = size.expected;
        }

        @SuppressWarnings( "boxing" )
        @Override
        ExpectInteger done( long value )
        {
            isDone = true;
            if ( expected != value )
            {
                throw new AssertionError( String.format( "expected: <%X> but was: <%X>", expected,
                        value ) );
            }
            return null;
        }
    }
}
