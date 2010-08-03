package org.neo4j.kernel.impl.ha;

import java.nio.channels.ReadableByteChannel;
import java.util.Collection;

/**
 * Represents a stream of the data of one or more consecutive transactions. 
 */
public final class TransactionStream
{
    private final Collection<ReadableByteChannel> channels;

    public TransactionStream( Collection<ReadableByteChannel> channels )
    {
        this.channels = channels;
    }

    public Collection<ReadableByteChannel> getChannels()
    {
        return channels;
    }
    
//    private byte[] readStream( ReadableByteChannel stream )
//    {
//        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
//        try
//        {
//            byte[] bytes = new byte[0];
//            while ( true )
//            {
//                buffer.clear();
//                int read = stream.read( buffer );
//                buffer.flip();
//                bytes = extend( bytes, buffer, read );
//            }
//        }
//        catch ( IOException e )
//        {
//            throw new RuntimeException( e );
//        }
//    }
//
//    private static byte[] extend( byte[] bytes, ByteBuffer buffer, int read )
//    {
//        byte[] result = new byte[bytes.length+read];
//        System.arraycopy( bytes, 0, result, 0, bytes.length );
//        System.arraycopy( buffer.array(), bytes.length, result, bytes.length, read );
//        return result;
//    }
}
