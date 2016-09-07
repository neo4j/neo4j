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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import static java.nio.file.Files.isRegularFile;

import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;
import static org.neo4j.dbms.archive.Utils.copy;

public class Dumper
{
    public void dump( Path root, Path archive ) throws IOException
    {
        checkWritableDirectory( archive.getParent() );
        try ( Stream<Path> files = Files.walk( root );
              ArchiveOutputStream stream = openArchiveOut( archive ) )
        {
            files.forEach( file -> dumpFile( file, root, stream ) );
        }
        catch ( TunnellingException e )
        {
            throw e.getWrapped();
        }
    }

    private static ArchiveOutputStream openArchiveOut( Path archive ) throws IOException
    {
        // StandardOpenOption.CREATE_NEW is important here because it atomically asserts that the file doesn't
        // exist as it is opened, avoiding a TOCTOU race condition which results in a security vulnerability. I
        // can't see a way to write a test to verify that we are using this option rather than just implementing
        // the check ourselves non-atomically.
        TarArchiveOutputStream tarball =
                new TarArchiveOutputStream( new GzipCompressorOutputStream(
                        Files.newOutputStream( archive, StandardOpenOption.CREATE_NEW ) ) );
        tarball.setLongFileMode( TarArchiveOutputStream.LONGFILE_POSIX );
        return tarball;
    }

    private static void dumpFile( Path file, Path root, ArchiveOutputStream archive )
    {
        try
        {
            ArchiveEntry entry = createEntry( file, root, archive );
            archive.putArchiveEntry( entry );
            if ( isRegularFile( file ) )
            {
                writeFile( file, archive );
            }
            archive.closeArchiveEntry();
        }
        catch ( IOException e )
        {
            throw new TunnellingException( e );
        }
    }

    private static ArchiveEntry createEntry( Path file, Path root, ArchiveOutputStream archive ) throws IOException
    {
        return archive.createArchiveEntry( file.toFile(), "./" + root.relativize( file ).toString() );
    }

    private static void writeFile( Path file, ArchiveOutputStream archiveStream ) throws IOException
    {
        try ( InputStream in = Files.newInputStream( file ) )
        {
            copy( in, archiveStream );
        }
    }

    private static class TunnellingException extends RuntimeException
    {
        public TunnellingException( IOException e )
        {
            super( e );
        }

        public IOException getWrapped()
        {
            return (IOException) getCause();
        }
    }
}
