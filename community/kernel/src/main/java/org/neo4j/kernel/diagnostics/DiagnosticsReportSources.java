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
package org.neo4j.kernel.diagnostics;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Contains helper methods to create create {@link DiagnosticsReportSource}.
 */
public final class DiagnosticsReportSources
{
    private DiagnosticsReportSources()
    {
        // util class
    }

    /**
     * Create a diagnostics source the will copy a file into the archive.
     *
     * @param destination final destination in archive.
     * @param fs filesystem abstraction to use.
     * @param source source file to archive
     * @return a diagnostics source consuming a file.
     */
    public static DiagnosticsReportSource newDiagnosticsFile( String destination, FileSystemAbstraction fs, Path source )
    {
        return new DiagnosticsFileReportSource( destination, fs, source );
    }

    /**
     * @param destinationFolder destination folder (including trailing '/') in archive.
     * @param fs filesystem abstraction to use.
     * @param file input log file, should be without rotation numbers.
     * @return a list diagnostics sources consisting of the log file including all rotated away files.
     */
    public static List<DiagnosticsReportSource> newDiagnosticsRotatingFile( String destinationFolder,
            FileSystemAbstraction fs, Path file )
    {
        List<DiagnosticsReportSource> files = new ArrayList<>();

        Path[] paths = fs.listFiles( file.getParent(), path -> path.getFileName().toString().startsWith( file.getFileName().toString() ) );

        if ( paths != null )
        {
            for ( Path path : paths )
            {
                files.add( newDiagnosticsFile( destinationFolder + path.getFileName().toString(), fs, path ) );
            }
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
    public static DiagnosticsReportSource newDiagnosticsString( String destination, Supplier<String> messageSupplier )
    {
        return new DiagnosticsStringReportSource( destination, messageSupplier );
    }

    private static class DiagnosticsFileReportSource implements DiagnosticsReportSource
    {
        private final String destination;
        private final FileSystemAbstraction fs;
        private final Path source;

        DiagnosticsFileReportSource( String destination, FileSystemAbstraction fs, Path source )
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
        public InputStream newInputStream() throws IOException
        {
            return fs.openAsInputStream( source );
        }

        @Override
        public long estimatedSize()
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
        public InputStream newInputStream()
        {
            final String message = messageSupplier.get();
            return new ByteArrayInputStream( message.getBytes( StandardCharsets.UTF_8 ) );
        }

        @Override
        public long estimatedSize()
        {
            return 0; // Size of strings should be negligible
        }
    }
}
