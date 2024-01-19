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
package org.neo4j.kernel.diagnostics;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.diagnostics.DiagnosticsReportSources.newDiagnosticsFile;

@TestDirectoryExtension
class DiagnosticsReporterTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Test
    void dumpFiles() throws Exception
    {
        DiagnosticsReporter reporter = setupDiagnosticsReporter();

        Path destination = testDirectory.file( "logs.zip" );

        reporter.dump( Collections.singleton( "logs" ), destination, mock( DiagnosticsReporterProgress.class), true );

        // Verify content
        verifyContent( destination );
    }

    @Test
    void shouldContinueAfterError() throws Exception
    {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        MyProvider myProvider = new MyProvider( fileSystem );
        reporter.registerOfflineProvider( myProvider );

        myProvider.addFile( "logs/a.txt", createNewFileWithContent( "a.txt", "file a") );

        Path destination = testDirectory.file( "logs.zip" );
        Set<String> classifiers = new HashSet<>();
        classifiers.add( "logs" );
        classifiers.add( "fail" );
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream out = new PrintStream( baos );
            NonInteractiveProgress progress = new NonInteractiveProgress( out, false );

            reporter.dump( classifiers, destination, progress, true );

            assertThat( baos.toString() ).isEqualTo( String.format(
                    "1/2 fail.txt%n" +
                            "%n" +
                            "Error: Failed to write fail.txt%n" +
                            "2/2 logs/a.txt%n" +
                            "....................  20%%%n" +
                            "....................  40%%%n" +
                            "....................  60%%%n" +
                            "....................  80%%%n" +
                            ".................... 100%%%n%n" ) );
        }

        // Verify content
        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath() );

        try ( FileSystem fs = FileSystems.newFileSystem( uri, Collections.emptyMap() ) )
        {
            List<String> fileA = Files.readAllLines( fs.getPath( "logs/a.txt" ) );
            assertEquals( 1, fileA.size() );
            assertEquals( "file a", fileA.get( 0 ) );
        }
    }

    @Test
    void supportPathsWithSpaces() throws IOException
    {
        DiagnosticsReporter reporter = setupDiagnosticsReporter();

        Path destination = testDirectory.file( "log files.zip" );

        reporter.dump( Collections.singleton( "logs" ), destination, mock( DiagnosticsReporterProgress.class), true );

        verifyContent( destination );
    }

    private Path createNewFileWithContent( String name, String content ) throws IOException
    {
        Path file = testDirectory.file( name );
        Files.write( file, content.getBytes() );
        return file;
    }

    private DiagnosticsReporter setupDiagnosticsReporter() throws IOException
    {
        DiagnosticsReporter reporter = new DiagnosticsReporter();
        MyProvider myProvider = new MyProvider( fileSystem );
        reporter.registerOfflineProvider( myProvider );

        myProvider.addFile( "logs/a.txt", createNewFileWithContent( "a.txt", "file a") );
        myProvider.addFile( "logs/b.txt", createNewFileWithContent( "b.txt", "file b") );
        return reporter;
    }

    private static void verifyContent( Path destination ) throws IOException
    {
        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getRawPath() );

        try ( FileSystem fs = FileSystems.newFileSystem( uri, Collections.emptyMap() ) )
        {
            List<String> fileA = Files.readAllLines( fs.getPath( "logs/a.txt" ) );
            assertEquals( 1, fileA.size() );
            assertEquals( "file a", fileA.get( 0 ) );

            List<String> fileB = Files.readAllLines( fs.getPath( "logs/b.txt" ) );
            assertEquals( 1, fileB.size() );
            assertEquals( "file b", fileB.get( 0 ) );
        }
    }

    private static class MyProvider extends DiagnosticsOfflineReportProvider
    {
        private final FileSystemAbstraction fs;
        private final List<DiagnosticsReportSource> logFiles = new ArrayList<>();

        MyProvider( FileSystemAbstraction fs )
        {
            super( "my-provider", "logs" );
            this.fs = fs;
        }

        void addFile( String destination, Path file )
        {
            logFiles.add( newDiagnosticsFile( destination, fs, file ) );
        }

        @Override
        public void init( FileSystemAbstraction fs, Config config, Set<String> databaseNames )
        {
        }

        @Override
        public List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
        {
            List<DiagnosticsReportSource> sources = new ArrayList<>();
            if ( classifiers.contains( "fail" ) )
            {
                sources.add( new FailingSource() );
            }
            if ( classifiers.contains( "logs" ) )
            {
                sources.addAll( logFiles );
            }

            return sources;
        }
    }
    private static class FailingSource implements DiagnosticsReportSource
    {

        @Override
        public String destinationPath()
        {
            return "fail.txt";
        }

        @Override
        public InputStream newInputStream()
        {
            throw new RuntimeException( "You had it coming..." );
        }

        @Override
        public long estimatedSize()
        {
            return 0;
        }
    }
}
