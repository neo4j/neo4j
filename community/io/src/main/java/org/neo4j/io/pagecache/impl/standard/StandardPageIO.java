package org.neo4j.io.pagecache.impl.standard;

import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;

public class StandardPageIO implements PageTable.PageIO
{
    private final StoreChannel channel;

    public StandardPageIO( StoreChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public void read( long pageId, ByteBuffer into )
    {

    }

    @Override
    public void write( long pageId, ByteBuffer from )
    {

    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        StandardPageIO that = (StandardPageIO) o;

        if ( channel != null ? !channel.equals( that.channel ) : that.channel != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return channel != null ? channel.hashCode() : 0;
    }
}
