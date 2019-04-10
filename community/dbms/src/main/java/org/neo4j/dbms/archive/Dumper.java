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
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.commandline.Util;
import org.neo4j.function.ThrowingAction;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.dbms.archive.Utils.checkWritableDirectory;
import static org.neo4j.dbms.archive.Utils.copy;
import static org.neo4j.function.Predicates.not;
import static org.neo4j.function.ThrowingAction.noop;
import static org.neo4j.io.fs.FileVisitors.justContinue;
import static org.neo4j.io.fs.FileVisitors.onDirectory;
import static org.neo4j.io.fs.FileVisitors.onFile;
import static org.neo4j.io.fs.FileVisitors.onlyMatching;
import static org.neo4j.io.fs.FileVisitors.throwExceptions;

public class Dumper
{
    private final List<ArchiveOperation> operations;
    private final ProgressPrinter progressPrinter;

    @VisibleForTesting
    Dumper()
    {
        operations = new ArrayList<>();
        progressPrinter = new ProgressPrinter( null );
    }

    public Dumper( PrintStream output )
    {
        operations = new ArrayList<>();
        progressPrinter = new ProgressPrinter( output );
    }

    public void dump( Path dbPath, Path transactionalLogsPath, Path archive, CompressionFormat format, Predicate<Path> exclude )
            throws IOException
    {
        checkWritableDirectory( archive.getParent() );
        try ( ArchiveOutputStream stream = openArchiveOut( archive, format ) )
        {
            operations.clear();

            visitPath( dbPath, exclude, stream );
            if ( !Util.isSameOrChildPath( dbPath, transactionalLogsPath ) )
            {
                visitPath( transactionalLogsPath, exclude, stream );
            }

            progressPrinter.reset();
            for ( ArchiveOperation operation : operations )
            {
                progressPrinter.maxBytes += operation.size;
                progressPrinter.maxFiles += operation.isFile ? 1 : 0;
            }

            ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
            ScheduledFuture<?> timerFuture = timer.scheduleAtFixedRate( progressPrinter::printOnNextUpdate, 0, 100, TimeUnit.MILLISECONDS );
            try
            {
                for ( ArchiveOperation operation : operations )
                {
                    operation.addToArchive();
                }
            }
            finally
            {
                timerFuture.cancel( false );
                timer.shutdown();
                try
                {
                    timer.awaitTermination( 10, TimeUnit.SECONDS );
                }
                catch ( InterruptedException ignore )
                {
                }
            }
            progressPrinter.done();
            progressPrinter.printProgress();
        }
    }

    private void visitPath( Path transactionalLogsPath, Predicate<Path> exclude, ArchiveOutputStream stream )
            throws IOException
    {
        Files.walkFileTree( transactionalLogsPath,
                onlyMatching( not( exclude ),
                        throwExceptions(
                                onDirectory( dir -> dumpDirectory( transactionalLogsPath, stream, dir ),
                                        onFile( file -> dumpFile( transactionalLogsPath, stream, file ),
                                                justContinue() ) ) ) ) );
    }

    private static ArchiveOutputStream openArchiveOut( Path archive, CompressionFormat format ) throws IOException
    {
        // StandardOpenOption.CREATE_NEW is important here because it atomically asserts that the file doesn't
        // exist as it is opened, avoiding a TOCTOU race condition which results in a security vulnerability. I
        // can't see a way to write a test to verify that we are using this option rather than just implementing
        // the check ourselves non-atomically.
        OutputStream out = Files.newOutputStream( archive, StandardOpenOption.CREATE_NEW );
        TarArchiveOutputStream tarball = new TarArchiveOutputStream( format.compress( out ) ) ;
        tarball.setLongFileMode( TarArchiveOutputStream.LONGFILE_POSIX );
        tarball.setBigNumberMode( TarArchiveOutputStream.BIGNUMBER_POSIX );
        return tarball;
    }

    private void dumpFile( Path root, ArchiveOutputStream stream, Path file ) throws IOException
    {
        withEntry( () -> writeFile( file, stream ), root, stream, file );
    }

    private void dumpDirectory( Path root, ArchiveOutputStream stream, Path dir ) throws IOException
    {
        withEntry( noop(), root, stream, dir );
    }

    private void withEntry( ThrowingAction<IOException> operation, Path root, ArchiveOutputStream stream, Path file )
            throws IOException
    {
        operations.add( new ArchiveOperation( operation, root, stream, file ) );
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
        final ThrowingAction<IOException> operation;
        final long size;
        final boolean isFile;
        final Path root;
        final ArchiveOutputStream stream;
        final Path file;

        private ArchiveOperation( ThrowingAction<IOException> operation, Path root, ArchiveOutputStream stream, Path file ) throws IOException
        {
            this.operation = operation;
            this.isFile = Files.isRegularFile( file );
            this.size = isFile ? Files.size( file ) : 0;
            this.root = root;
            this.stream = stream;
            this.file = file;
        }

        void addToArchive() throws IOException
        {
            ArchiveEntry entry = createEntry( file, root, stream );
            stream.putArchiveEntry( entry );
            operation.apply();
            stream.closeArchiveEntry();
        }

        private ArchiveEntry createEntry( Path file, Path root, ArchiveOutputStream archive ) throws IOException
        {
            return archive.createArchiveEntry( file.toFile(), "./" + root.relativize( file ).toString() );
        }
    }
}
