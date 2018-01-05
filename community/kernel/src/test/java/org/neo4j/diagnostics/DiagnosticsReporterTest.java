/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.diagnostics;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.diagnostics.DiagnosticsReportSources.newDiagnosticsFile;

public class DiagnosticsReporterTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    static class MyProvider extends DiagnosticsOfflineReportProvider
    {
        private final FileSystemAbstraction fs;
        private List<DiagnosticsReportSource> logFiles = new ArrayList<>();

        MyProvider( FileSystemAbstraction fs )
        {
            super( "my-provider", "logs" );
            this.fs = fs;
        }

        void addFile( String destination, File file )
        {
            logFiles.add( newDiagnosticsFile( destination, fs, file ) );
        }

        @Override
        public void init( FileSystemAbstraction fs, Config config, File storeDirectory )
        {
        }

        @Override
        public List<DiagnosticsReportSource> provideSources( Set<String> classifiers )
        {
            if ( classifiers.contains( "logs" ) )
            {
                return logFiles;
            }

            return Collections.emptyList();
        }
    }

    @Test
    public void dumpFiles() throws Exception
    {
        DiagnosticsReporter reporter = new DiagnosticsReporter(  );
        MyProvider myProvider = new MyProvider( fileSystemRule.get() );
        reporter.registerOfflineProvider( myProvider );

        myProvider.addFile( "logs/a.txt", createNewFileWithContent( "a.txt", "file a") );
        myProvider.addFile( "logs/b.txt", createNewFileWithContent( "b.txt", "file b") );

        Path destination = testDirectory.file( "logs.zip" ).toPath();

        reporter.dump( Collections.singleton( "logs" ), destination, mock( DiagnosticsReporterProgressCallback.class ) );

        // Verify content
        URI uri = URI.create("jar:file:" + destination.toAbsolutePath().toUri().getPath() );

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

    private File createNewFileWithContent( String name, String content ) throws IOException
    {
        Path file = testDirectory.file( name ).toPath();
        Files.write( file, content.getBytes() );
        return file.toFile();
    }
}
