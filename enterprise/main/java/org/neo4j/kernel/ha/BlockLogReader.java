package org.neo4j.kernel.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;

public class BlockLogReader implements ReadableByteChannel
{
    private final ChannelBuffer source;
    private final byte[] byteArray = new byte[BlockLogBuffer.MAX_SIZE];
    private final ByteBuffer byteBuffer = ByteBuffer.wrap( byteArray );
    private boolean moreBlocks;
    
    public BlockLogReader( ChannelBuffer source )
    {
        this.source = source;
        readNextBlock();
    }
    
    private void readNextBlock()
    {
        int maxReadableBytes = source.readableBytes();
        if ( maxReadableBytes > 0 )
        {
            byteBuffer.clear();
            byteBuffer.limit( Math.min( byteBuffer.capacity(), maxReadableBytes ) );
            source.readBytes( byteBuffer );
            byteBuffer.flip();
            moreBlocks = byteBuffer.get() == BlockLogBuffer.FULL_BLOCK_AND_MORE;
        }
    }

    public boolean isOpen()
    {
        return true;
    }

    public void close() throws IOException
    {
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        int bytesWanted = dst.limit();
        int bytesRead = 0;
        while ( bytesWanted > 0 )
        {
            int bytesReadThisTime = readAsMuchAsPossible( dst, bytesWanted );
            if ( bytesReadThisTime == 0 )
            {
                break;
            }
            bytesRead += bytesReadThisTime;
            bytesWanted -= bytesReadThisTime;
        }
        return bytesRead;
    }

    private int readAsMuchAsPossible( ByteBuffer dst, int maxBytesWanted )
    {
        if ( byteBuffer.remaining() == 0 && moreBlocks )
        {
            readNextBlock();
        }
        
        int bytesToRead = Math.min( maxBytesWanted, byteBuffer.remaining() );
        dst.put( byteArray, byteBuffer.position(), bytesToRead );
        byteBuffer.position( byteBuffer.position()+bytesToRead );
        return bytesToRead;
    }
}
