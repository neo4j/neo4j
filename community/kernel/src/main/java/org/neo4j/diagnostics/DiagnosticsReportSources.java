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
package org.neo4j.diagnostics;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;

import static org.neo4j.logging.RotatingFileOutputStreamSupplier.getAllArchives;

/**
 * Contains helper methods to create create {@link DiagnosticsReportSource}.
 */
public class DiagnosticsReportSources
{
    private DiagnosticsReportSources()
    {
        throw new AssertionError( "No instances" );
    }

    /**
     * Create a diagnostics source the will copy a file into the archive.
     *
     * @param destination final destination in archive.
     * @param fs filesystem abstraction to use.
     * @param source source file to archive
     * @return a diagnostics source consuming a file.
     */
    public static DiagnosticsReportSource newDiagnosticsFile( String destination, FileSystemAbstraction fs,
            File source )
    {
        return new DiagnosticsFileReportSource( destination, fs, source );
    }

    /**
     * This is to be used by loggers that uses {@link RotatingFileOutputStreamSupplier}.
     *
     * @param destination final destination in archive.
     * @param fs filesystem abstraction to use.
     * @param file input log file, should be without rotation numbers.
     * @return a list diagnostics sources consisting of the log file including all rotated away files.
     */
    public static List<DiagnosticsReportSource> newDiagnosticsRotatingFile( String destination,
            FileSystemAbstraction fs, File file )
    {
        ArrayList<DiagnosticsReportSource> files = new ArrayList<>();

        files.add( newDiagnosticsFile( destination, fs, file ) );

        List<File> allArchives = getAllArchives( fs, file );
        for ( File archive : allArchives )
        {
            String name = archive.getName();
            String n = name.substring( name.lastIndexOf( '.' ) );
            files.add( newDiagnosticsFile( destination + "." + n, fs, archive ) );
        }
        return files;
    }

    /**
     * Create a diagnostics source from a string. Can be used to dump simple messages to a file in the archive. Files
     * are opened with append option so this method can be used to accumulate messages from multiple source to a single
     * file in the archive.
     *
     * @param destination final destination in archive.
     * @param messageSupplier a string supplier with the final message.
     * @return a diagnostics source consuming a string.
     */
    public static DiagnosticsReportSource newDiagnosticsString( String destination,
            Supplier<String> messageSupplier )
    {
        return new DiagnosticsStringReportSource( destination, messageSupplier );
    }

    private static class DiagnosticsFileReportSource implements DiagnosticsReportSource
    {
        private final String destination;
        private final FileSystemAbstraction fs;
        private final File source;

        DiagnosticsFileReportSource( String destination, FileSystemAbstraction fs, File source )
        {
            this.destination = destination;
            this.fs = fs;
            this.source = source;
        }

        @Override
        public String destinationPath()
        {
            return destination;
        }

        @Override
        public void addToArchive( Path archiveDestination, DiagnosticsReporterProgress progress )
                throws IOException
        {
            long size = fs.getFileSize( source );
            InputStream in = fs.openAsInputStream( source );

            // Track progress of the file reading, source might be a very large file
            try ( ProgressAwareInputStream inStream = new ProgressAwareInputStream( in, size, progress::percentChanged ) )
            {
                Files.copy( inStream, archiveDestination );
            }
        }

        @Override
        public long estimatedSize( DiagnosticsReporterProgress progress )
        {
            return fs.getFileSize( source );
        }
    }

    private static class DiagnosticsStringReportSource implements DiagnosticsReportSource
    {
        private final String destination;
        private final Supplier<String> messageSupplier;

        private DiagnosticsStringReportSource( String destination, Supplier<String> messageSupplier )
        {
            this.destination = destination;
            this.messageSupplier = messageSupplier;
        }

        @Override
        public String destinationPath()
        {
            return destination;
        }

        @Override
        public void addToArchive( Path archiveDestination, DiagnosticsReporterProgress progress )
                throws IOException
        {
            String message = messageSupplier.get();
            Files.write( archiveDestination, message.getBytes( StandardCharsets.UTF_8 ), StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND );
        }

        @Override
        public long estimatedSize( DiagnosticsReporterProgress progress )
        {
            return 0; // Size of strings should be negligible
        }
    }
}
