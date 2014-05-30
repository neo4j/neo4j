/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.function.primitive.FunctionFromPrimitiveLong;
import org.neo4j.io.fs.StoreChannel;

public class StandardPageIO implements PageIO
{
    private final File file;
    private final StoreChannel channel;
    private final int filePageSize;
    private final FunctionFromPrimitiveLong onEviction;

    public StandardPageIO( File file, StoreChannel channel, int filePageSize, FunctionFromPrimitiveLong onEviction )
    {
        this.file = file;
        this.channel = channel;
        this.filePageSize = filePageSize;
        this.onEviction = onEviction;
    }

    @Override
    public void read( long pageId, ByteBuffer into ) throws IOException
    {
        into.position( 0 );
        into.limit( filePageSize );
        long position = pageIdToPosition( pageId );

        int readBytes = -1;
        if ( position <= channel.size() )
        {
            readBytes = channel.read( into, position );
        }

        if ( readBytes < filePageSize )
        {
            // zero-fill any part of the page that was not in the file.
            for ( int i = Math.max( readBytes, 0 ); i < filePageSize; i++ )
            {
                into.put( i, (byte) 0 );
            }
        }
    }

    @Override
    public void write( long pageId, ByteBuffer from ) throws IOException
    {
        from.position(0);
        from.limit( filePageSize );
        channel.writeAll( from, pageIdToPosition( pageId ) );
    }

    @Override
    public void evicted( long pageId )
    {
        onEviction.apply( pageId );
    }

    @Override
    public String fileName()
    {
        return file.getName();
    }

    private long pageIdToPosition( long pageId )
    {
        return filePageSize * pageId;
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

    public void close() throws IOException
    {
        channel.close();
    }

    public void force() throws IOException
    {
        channel.force( false );
    }
}
