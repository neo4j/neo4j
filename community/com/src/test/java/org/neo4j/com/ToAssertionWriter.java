package org.neo4j.com;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ToAssertionWriter implements MadeUpWriter
{
    private int index;
    
    @Override
    public void write( ReadableByteChannel data )
    {
        ByteBuffer intermediate = ByteBuffer.allocate( 1000 );
        while ( true )
        {
            try
            {
                intermediate.clear();
                if ( data.read( intermediate ) == -1 )
                {
                    break;
                }
                intermediate.flip();
                while ( intermediate.remaining() > 0 )
                {
                    byte value = intermediate.get();
                    assertEquals( (index++)%10, value );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}