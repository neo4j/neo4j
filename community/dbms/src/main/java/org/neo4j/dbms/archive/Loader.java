/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.dbms.archive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import static java.nio.file.Files.exists;

import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;

public class Loader
{
    public void load( Path archive, Path destination ) throws IOException, IncorrectFormat
    {
        if ( exists( destination ) )
        {
            throw new FileAlreadyExistsException( destination.toString() );
        }
        checkWritableDirectory( destination.getParent() );
        try ( ArchiveInputStream stream = openArchiveIn( archive ) )
        {
            ArchiveEntry entry;
            while ( (entry = nextEntry( stream, archive )) != null )
            {
                loadEntry( destination, stream, entry );
            }
        }
    }

    private ArchiveEntry nextEntry( ArchiveInputStream stream, Path archive ) throws IncorrectFormat
    {
        try
        {
            return stream.getNextEntry();
        }
        catch ( IOException e )
        {
            throw new IncorrectFormat( archive, e );
        }
    }

    private void loadEntry( Path destination, ArchiveInputStream stream, ArchiveEntry entry ) throws IOException
    {
        Path file = destination.resolve( entry.getName() );
        if ( entry.isDirectory() )
        {
            Files.createDirectories( file );
        }
        else
        {
            try ( OutputStream output = Files.newOutputStream( file ) )
            {
                Utils.copy( stream, output );
            }
        }
    }

    private static ArchiveInputStream openArchiveIn( Path archive ) throws IOException, IncorrectFormat
    {
        InputStream input = Files.newInputStream( archive );
        GzipCompressorInputStream compressor;
        try
        {
            compressor = new GzipCompressorInputStream( input );
        }
        catch ( IOException e )
        {
            input.close();
            throw new IncorrectFormat( archive, e );
        }
        return new TarArchiveInputStream( compressor );
    }
}
