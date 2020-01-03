/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.neo4j.commandline.Util;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Resource;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;
import static org.neo4j.dbms.archive.Utils.copy;
import static org.neo4j.function.Predicates.not;
import static org.neo4j.io.fs.FileVisitors.justContinue;
import static org.neo4j.io.fs.FileVisitors.onDirectory;
import static org.neo4j.io.fs.FileVisitors.onFile;
import static org.neo4j.io.fs.FileVisitors.onlyMatching;
import static org.neo4j.io.fs.FileVisitors.throwExceptions;

public class Dumper
{
    private final List<ArchiveOperation> operations;
    private final ArchiveProgressPrinter progressPrinter;

    @VisibleForTesting
    Dumper()
    {
        operations = new ArrayList<>();
        progressPrinter = new ArchiveProgressPrinter( null );
    }

    public Dumper( PrintStream output )
    {
        operations = new ArrayList<>();
        progressPrinter = new ArchiveProgressPrinter( output );
    }

    public void dump( Path dbPath, Path transactionalLogsPath, Path archive, CompressionFormat format, Predicate<Path> exclude ) throws IOException
    {
        checkWritableDirectory( archive.getParent() );
        operations.clear();

        visitPath( dbPath, exclude );
        if ( !Util.isSameOrChildPath( dbPath, transactionalLogsPath ) )
        {
            visitPath( transactionalLogsPath, exclude );
        }

        progressPrinter.reset();
        for ( ArchiveOperation operation : operations )
        {
            progressPrinter.maxBytes += operation.size;
            progressPrinter.maxFiles += operation.isFile ? 1 : 0;
        }

        try ( ArchiveOutputStream stream = openArchiveOut( archive, format );
              Resource ignore = progressPrinter.startPrinting() )
        {
            for ( ArchiveOperation operation : operations )
            {
                operation.addToArchive( stream );
            }
        }
    }

    private void visitPath( Path transactionalLogsPath, Predicate<Path> exclude ) throws IOException
    {
        Files.walkFileTree( transactionalLogsPath,
                onlyMatching( not( exclude ),
                        throwExceptions(
                                onDirectory( dir -> dumpDirectory( transactionalLogsPath, dir ),
                                        onFile( file -> dumpFile( transactionalLogsPath, file ),
                                                justContinue() ) ) ) ) );
    }

    private ArchiveOutputStream openArchiveOut( Path archive, CompressionFormat format ) throws IOException
    {
        // StandardOpenOption.CREATE_NEW is important here because it atomically asserts that the file doesn't
        // exist as it is opened, avoiding a TOCTOU race condition which results in a security vulnerability. I
        // can't see a way to write a test to verify that we are using this option rather than just implementing
        // the check ourselves non-atomically.
        OutputStream out = Files.newOutputStream( archive, StandardOpenOption.CREATE_NEW );
        OutputStream compress = format.compress( out );

        // Add enough archive meta-data that the load command can print a meaningful progress indicator.
        if ( format == CompressionFormat.ZSTD )
        {
            writeArchiveMetadata( compress );
        }

        TarArchiveOutputStream tarball = new TarArchiveOutputStream( compress ) ;
        tarball.setLongFileMode( TarArchiveOutputStream.LONGFILE_POSIX );
        tarball.setBigNumberMode( TarArchiveOutputStream.BIGNUMBER_POSIX );
        return tarball;
    }

    /**
     * @see Loader#readArchiveMetadata(InputStream)
     */
    void writeArchiveMetadata( OutputStream stream ) throws IOException
    {
        DataOutputStream metadata = new DataOutputStream( stream ); // Unbuffered. No need for flushing.
        metadata.writeInt( 1 ); // Archive format version. Increment whenever the metadata format changes.
        metadata.writeLong( progressPrinter.maxFiles );
        metadata.writeLong( progressPrinter.maxBytes );
    }

    private void dumpFile( Path root, Path file ) throws IOException
    {
        withEntry( stream -> writeFile( file, stream ), root, file );
    }

    private void dumpDirectory( Path root, Path dir ) throws IOException
    {
        withEntry( stream -> {}, root, dir );
    }

    private void withEntry( ThrowingConsumer<ArchiveOutputStream, IOException> operation, Path root, Path file ) throws IOException
    {
        operations.add( new ArchiveOperation( operation, root, file ) );
    }

    private void writeFile( Path file, ArchiveOutputStream archiveStream ) throws IOException
    {
        try ( InputStream in = Files.newInputStream( file ) )
        {
            copy( in, archiveStream, progressPrinter );
        }
    }

    private static class ArchiveOperation
    {
        final ThrowingConsumer<ArchiveOutputStream, IOException> operation;
        final long size;
        final boolean isFile;
        final Path root;
        final Path file;

        private ArchiveOperation( ThrowingConsumer<ArchiveOutputStream, IOException> operation, Path root, Path file ) throws IOException
        {
            this.operation = operation;
            this.isFile = Files.isRegularFile( file );
            this.size = isFile ? Files.size( file ) : 0;
            this.root = root;
            this.file = file;
        }

        void addToArchive( ArchiveOutputStream stream ) throws IOException
        {
            ArchiveEntry entry = createEntry( file, root, stream );
            stream.putArchiveEntry( entry );
            operation.accept( stream );
            stream.closeArchiveEntry();
        }

        private ArchiveEntry createEntry( Path file, Path root, ArchiveOutputStream archive ) throws IOException
        {
            return archive.createArchiveEntry( file.toFile(), "./" + root.relativize( file ).toString() );
        }
    }
}
