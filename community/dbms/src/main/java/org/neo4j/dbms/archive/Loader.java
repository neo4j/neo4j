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
package org.neo4j.dbms.archive;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.util.VisibleForTesting;

import static java.nio.file.Files.exists;
import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;

public class Loader
{
    private final ArchiveProgressPrinter progressPrinter;

    @VisibleForTesting
    Loader()
    {
        progressPrinter = new ArchiveProgressPrinter( null );
    }

    public Loader( PrintStream output )
    {
        progressPrinter = new ArchiveProgressPrinter( output );
    }

    public void load( Path archive, Path databaseDestination, Path transactionLogsDirectory ) throws IOException, IncorrectFormat
    {
        validatePath( databaseDestination );
        validatePath( transactionLogsDirectory );

        createDestination( databaseDestination );
        createDestination( transactionLogsDirectory );

        try ( ArchiveInputStream stream = openArchiveIn( archive );
              Resource ignore = progressPrinter.startPrinting() )
        {
            ArchiveEntry entry;
            while ( (entry = nextEntry( stream, archive )) != null )
            {
                Path destination = determineEntryDestination( entry, databaseDestination, transactionLogsDirectory );
                loadEntry( destination, stream, entry );
            }
        }
    }

    private void createDestination( Path destination ) throws IOException
    {
        if ( !destination.toFile().exists() )
        {
            Files.createDirectories( destination );
        }
    }

    private void validatePath( Path path ) throws FileSystemException
    {
        if ( exists( path ) )
        {
            throw new FileAlreadyExistsException( path.toString() );
        }
        checkWritableDirectory( path.getParent() );
    }

    private static Path determineEntryDestination( ArchiveEntry entry, Path databaseDestination,
            Path transactionLogsDirectory )
    {
        String entryName = Paths.get( entry.getName() ).getFileName().toString();
        return TransactionLogFiles.DEFAULT_FILENAME_FILTER.accept( null, entryName ) ? transactionLogsDirectory
                                                                                           : databaseDestination;
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

        if ( !file.normalize().startsWith( destination ) )
        {
            throw new IOException( "Zip entry outside destination path." );
        }

        if ( entry.isDirectory() )
        {
            Files.createDirectories( file );
        }
        else
        {
            try ( OutputStream output = Files.newOutputStream( file ) )
            {
                Utils.copy( stream, output, progressPrinter );
            }
        }
    }

    private ArchiveInputStream openArchiveIn( Path archive ) throws IOException, IncorrectFormat
    {
        InputStream input = Files.newInputStream( archive );
        InputStream decompressor;
        try
        {
            decompressor = CompressionFormat.GZIP.decompress( input );
        }
        catch ( IOException e )
        {
            input.close(); // Reopen to reset file position.
            input = Files.newInputStream( archive );
            try
            {
                decompressor = CompressionFormat.ZSTD.decompress( input );
                // Important: Only the ZSTD compressed archives have any archive metadata.
                readArchiveMetadata( decompressor );
            }
            catch ( IOException ex )
            {
                input.close();
                ex.addSuppressed( e );
                throw new IncorrectFormat( archive, ex );
            }
        }

        return new TarArchiveInputStream( decompressor );
    }

    /**
     * @see Dumper#writeArchiveMetadata(OutputStream)
     */
    void readArchiveMetadata( InputStream stream ) throws IOException
    {
        DataInputStream metadata = new DataInputStream( stream ); // Unbuffered. Will not play naughty tricks with the file position.
        int version = metadata.readInt();
        if ( version == 1 )
        {
            progressPrinter.maxFiles = metadata.readLong();
            progressPrinter.maxBytes = metadata.readLong();
        }
        else
        {
            throw new IOException( "Cannot read archive meta-data. I don't recognise this archive version: " + version + "." );
        }
    }
}
