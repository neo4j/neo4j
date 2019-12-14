/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.test.limited;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.StoreChannel;

public class LimitedFileChannel extends DelegatingStoreChannel implements StoreChannel
{
    private final LimitedFilesystemAbstraction fs;

    public LimitedFileChannel( StoreChannel inner, LimitedFilesystemAbstraction limitedFilesystemAbstraction )
    {
        super( inner );
        fs = limitedFilesystemAbstraction;
    }

    @Override
    public int write( ByteBuffer byteBuffer ) throws IOException
    {
        fs.ensureHasSpace();
        return super.write( byteBuffer );
    }

    @Override
    public long write( ByteBuffer[] byteBuffers, int offset, int length ) throws IOException
    {
        fs.ensureHasSpace();
        return super.write( byteBuffers, offset, length );
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        fs.ensureHasSpace();
        return super.write( srcs );
    }

    @Override
    public LimitedFileChannel position( long newPosition ) throws IOException
    {
        super.position( newPosition );
        return this;
    }

    @Override
    public LimitedFileChannel truncate( long size ) throws IOException
    {
        super.truncate( size );
        return this;
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        fs.ensureHasSpace();
        super.force( metaData );
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        fs.ensureHasSpace();
        super.writeAll( src, position );
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        fs.ensureHasSpace();
        super.writeAll( src );
    }
}
