package org.neo4j.kernel.ha.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

class ByteArrayIteratorChannel implements ReadableByteChannel
{
    private int pos = 0;
    private byte[] current;
    private Iterator<byte[]> iterator;

    ByteArrayIteratorChannel( Iterator<byte[]> iterator )
    {
        if ( iterator.hasNext() )
        {
            this.current = iterator.next();
        }
        else
        {
            this.current = new byte[0];
        }
        this.iterator = iterator;
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        if ( pos >= current.length && !iterator.hasNext() ) return -1;
        int size = 0;
        while ( dst.hasRemaining() && ( pos < current.length || iterator.hasNext() ) )
        {
            if ( pos < current.length )
            {
                current = iterator.next();
                pos = 0;
            }
            int length = Math.min( current.length - pos, dst.limit() - dst.position() );
            dst.put( current, pos, length );
            pos += length;
            size += length;
        }
        return size;
    }

    public void close() throws IOException
    {
        current = null;
        iterator = null;
    }

    public boolean isOpen()
    {
        return current != null;
    }

}
