/*
 * Copyright (c) "Neo4j"
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
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.neo4j.commandline.dbms.StoreVersionLoader;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.archive.printer.OutputProgressPrinter;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.util.VisibleForTesting;

import static java.nio.file.Files.exists;
import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;
import static org.neo4j.dbms.archive.printer.ProgressPrinters.emptyPrinter;
import static org.neo4j.dbms.archive.printer.ProgressPrinters.printStreamPrinter;

public class Loader
{
    private final ArchiveProgressPrinter progressPrinter;

    @VisibleForTesting
    public Loader()
    {
        this( emptyPrinter() );
    }

    public Loader( PrintStream output )
    {
        this( printStreamPrinter( output ) );
    }

    private Loader( OutputProgressPrinter progressPrinter )
    {
        this.progressPrinter = new ArchiveProgressPrinter( progressPrinter );
    }

    public void load( DatabaseLayout databaseLayout, ThrowingSupplier<InputStream,IOException> streamSupplier ) throws IOException, IncorrectFormat
    {
        load( databaseLayout, streamSupplier, "" );
    }

    public void load( DatabaseLayout databaseLayout, ThrowingSupplier<InputStream,IOException> streamSupplier,
                      String inputName ) throws IOException, IncorrectFormat
    {
        load( databaseLayout, false, true, DumpFormatSelector::decompress, streamSupplier, inputName );
    }

    public void load( Path archive, DatabaseLayout databaseLayout, boolean validateDatabaseExistence, boolean validateLogsExistence,
                      DecompressionSelector selector ) throws IOException, IncorrectFormat
    {
        load( databaseLayout, validateDatabaseExistence, validateLogsExistence, selector, () -> Files.newInputStream( archive ), archive.toString() );
    }

    public void load( DatabaseLayout databaseLayout, boolean validateDatabaseExistence, boolean validateLogsExistence,
                      DecompressionSelector selector, ThrowingSupplier<InputStream,IOException> streamSupplier, String inputName )
            throws IOException, IncorrectFormat
    {
        Path databaseDestination = databaseLayout.databaseDirectory();
        Path transactionLogsDirectory = databaseLayout.getTransactionLogsDirectory();

        validatePath( databaseDestination, validateDatabaseExistence );
        validatePath( transactionLogsDirectory, validateLogsExistence );

        createDestination( databaseDestination );
        createDestination( transactionLogsDirectory );

        checkDatabasePresence( databaseLayout );

        try ( ArchiveInputStream stream = openArchiveIn( selector, streamSupplier, inputName );
              Resource ignore = progressPrinter.startPrinting() )
        {
            ArchiveEntry entry;
            while ( (entry = nextEntry( stream, inputName )) != null )
            {
                Path destination = determineEntryDestination( entry, databaseDestination, transactionLogsDirectory );
                loadEntry( destination, stream, entry );
            }
        }
    }

    public StoreVersionLoader.Result getStoreVersion( FileSystemAbstraction fs, Config config, DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory )
    {
        try ( StoreVersionLoader stl = new StoreVersionLoader( fs, config, contextFactory ) )
        {
            return stl.loadStoreVersion( databaseLayout );
        }
    }

    public DumpMetaData getMetaData( ThrowingSupplier<InputStream,IOException> streamSupplier ) throws IOException
    {
        try ( InputStream decompressor = DumpFormatSelector.decompress( streamSupplier ) )
        {
            String format = "TAR+GZIP.";
            String files = "?";
            String bytes = "?";
            if ( StandardCompressionFormat.ZSTD.isFormat( decompressor ) )
            {
                format = "Neo4j ZSTD Dump.";
                // Important: Only the ZSTD compressed archives have any archive metadata.
                readArchiveMetadata( decompressor );
                files = String.valueOf( progressPrinter.maxFiles );
                bytes = String.valueOf( progressPrinter.maxBytes );
            }
            return new DumpMetaData( format, files, bytes );
        }
    }

    private static void checkDatabasePresence( DatabaseLayout databaseLayout ) throws FileAlreadyExistsException
    {
        if ( Files.exists( databaseLayout.metadataStore() ) )
        {
            throw new FileAlreadyExistsException( databaseLayout.metadataStore().toAbsolutePath().toString() );
        }
    }

    private static void createDestination( Path destination ) throws IOException
    {
        if ( !Files.exists( destination ) )
        {
            Files.createDirectories( destination );
        }
    }

    private static void validatePath( Path path, boolean validateExistence ) throws FileSystemException
    {
        if ( validateExistence && exists( path ) )
        {
            throw new FileAlreadyExistsException( path.toString() );
        }
        checkWritableDirectory( path.getParent() );
    }

    private static Path determineEntryDestination( ArchiveEntry entry, Path databaseDestination,
            Path transactionLogsDirectory )
    {
        Path entryName = Path.of( entry.getName() ).getFileName();
        try
        {
            return TransactionLogFiles.DEFAULT_FILENAME_FILTER.accept( entryName ) ? transactionLogsDirectory
                                                                                         : databaseDestination;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static ArchiveEntry nextEntry( ArchiveInputStream stream, String inputName ) throws IncorrectFormat
    {
        try
        {
            return stream.getNextEntry();
        }
        catch ( IOException e )
        {
            throw new IncorrectFormat( inputName, e );
        }
    }

    private void loadEntry( Path destination, ArchiveInputStream stream, ArchiveEntry entry ) throws IOException
    {
        Path file = destination.resolve( entry.getName() );
        if ( !file.normalize().startsWith( destination ) )
        {
            throw new InvalidDumpEntryException( entry.getName() );
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

    private ArchiveInputStream openArchiveIn( DecompressionSelector selector, ThrowingSupplier<InputStream,IOException> streamSupplier, String inputName )
            throws IOException, IncorrectFormat
    {
        try
        {
            InputStream decompressor = selector.decompress( streamSupplier );

            if ( StandardCompressionFormat.ZSTD.isFormat( decompressor ) )
            {
                // Important: Only the ZSTD compressed archives have any archive metadata.
                readArchiveMetadata( decompressor );
            }

            return new TarArchiveInputStream( decompressor );
        }
        catch ( NoSuchFileException ioe )
        {
            throw ioe;
        }
        catch ( IOException e )
        {
            throw new IncorrectFormat( inputName, e );
        }
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

    public record DumpMetaData( String format, String fileCount, String byteCount )
    {
    }
}
