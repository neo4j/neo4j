/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class FileChunk
{
    static final int MAX_SIZE = 8192;
    private static final int USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS = -1;
    private final int encodedLength;
    private final byte[] bytes;

    static FileChunk create( byte[] bytes, boolean last )
    {
        if ( !last && bytes.length != MAX_SIZE )
        {
            throw new IllegalArgumentException( "All chunks except for the last must be of max size." );
        }
        return new FileChunk( last ? bytes.length : USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS, bytes );
    }

    FileChunk( int encodedLength, byte[] bytes )
    {
        this.encodedLength = encodedLength;
        this.bytes = bytes;
    }

    public boolean isLast()
    {
        return encodedLength != USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS;
    }

    public byte[] bytes()
    {
        return bytes;
    }

    public int length()
    {
        return encodedLength;
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
        FileChunk fileChunk = (FileChunk) o;
        return encodedLength == fileChunk.encodedLength && Arrays.equals( bytes, fileChunk.bytes );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( encodedLength, Arrays.hashCode( bytes ) );
    }

    @Override
    public String toString()
    {
        return "FileChunk{" + Arrays.toString( bytes ) + '}';
    }

    public static ChannelMarshal<FileChunk> marshal()
    {
        return Marshal.INSTANCE;
    }

    private static class Marshal extends SafeChannelMarshal<FileChunk>
    {
        private static final Marshal INSTANCE = new Marshal();

        private Marshal()
        {
        }

        @Override
        public void marshal( FileChunk fileChunk, WritableChannel channel ) throws IOException
        {
            channel.putInt( fileChunk.encodedLength );
            byte[] bytes = fileChunk.bytes();
            channel.put( bytes, bytes.length );
        }

        @Override
        protected FileChunk unmarshal0( ReadableChannel channel ) throws IOException
        {
            int encodedLength = channel.getInt();
            int length = encodedLength == USE_MAX_SIZE_AND_EXPECT_MORE_CHUNKS ? MAX_SIZE : encodedLength;
            byte[] bytes = new byte[length];
            channel.get( bytes, length );
            return new FileChunk( encodedLength, bytes );
        }
    }
}
