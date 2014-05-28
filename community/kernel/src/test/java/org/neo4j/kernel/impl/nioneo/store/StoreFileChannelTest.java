package org.neo4j.kernel.impl.nioneo.store;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;

import static org.mockito.Mockito.*;

public class StoreFileChannelTest
{

    @Test
    public void shouldHandlePartialWrites() throws Exception
    {
        // Given
        FileChannel mockChannel = mock(FileChannel.class);
        when(mockChannel.write( any(ByteBuffer.class), anyLong() )).thenReturn( 4 );

        ByteBuffer buffer = ByteBuffer.wrap( "Hello, world!".getBytes() );

        StoreFileChannel channel = new StoreFileChannel( mockChannel );

        // When
        channel.writeAll( buffer, 20 );

        // Then
        verify( mockChannel ).write( buffer, 20 );
        verify( mockChannel ).write( buffer, 24 );
        verify( mockChannel ).write( buffer, 28 );
        verify( mockChannel ).write( buffer, 32 );
        verifyNoMoreInteractions( mockChannel );
    }

}
