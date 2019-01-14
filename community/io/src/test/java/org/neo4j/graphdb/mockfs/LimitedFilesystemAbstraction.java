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
package org.neo4j.graphdb.mockfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;

public class LimitedFilesystemAbstraction extends DelegatingFileSystemAbstraction
{
    private volatile boolean outOfSpace;

    public LimitedFilesystemAbstraction( FileSystemAbstraction delegate )
    {
        super( delegate );
    }

    @Override
    public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
    {
        return new LimitedFileChannel( super.open( fileName, openMode ), this );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new ChannelOutputStream( open( fileName, OpenMode.READ_WRITE ), append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new ChannelInputStream( open( fileName, OpenMode.READ ) );
    }

    @Override
    public Reader openAsReader( File fileName, Charset charset ) throws IOException
    {
        return new InputStreamReader( openAsInputStream( fileName ), charset );
    }

    @Override
    public Writer openAsWriter( File fileName, Charset charset, boolean append ) throws IOException
    {
        return new OutputStreamWriter( openAsOutputStream( fileName, append ) );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        ensureHasSpace();
        return new LimitedFileChannel( super.create( fileName ), this );
    }

    @Override
    public void mkdirs( File fileName ) throws IOException
    {
        ensureHasSpace();
        super.mkdirs( fileName );
    }

    @Override
    public void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        ensureHasSpace();
        super.renameFile( from, to, copyOptions );
    }

    public void runOutOfDiskSpace( boolean outOfSpace )
    {
        this.outOfSpace = outOfSpace;
    }

    public void ensureHasSpace() throws IOException
    {
        if ( outOfSpace )
        {
            throw new IOException( "No space left on device" );
        }
    }
}
