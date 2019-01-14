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
package org.neo4j.commandline.dbms;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class DiagnosticsReportCommandIT
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private GraphDatabaseService database;

    @Before
    public void setUp()
    {
        database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .newGraphDatabase();
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void shouldBeAbleToAttachToPidAndRunThreadDump() throws IOException, CommandFailed, IncorrectUsage
    {
        long pid = getPID();
        assertThat( pid, is( not( 0 ) ) );

        // Write config file
        Files.createFile( testDirectory.file( "neo4j.conf" ).toPath() );

        // write neo4j.pid file
        File run = testDirectory.directory( "run" );
        Files.write( Paths.get( run.getAbsolutePath(), "neo4j.pid" ), String.valueOf( pid ).getBytes() );

        // Run command, should detect running instance
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            String[] args = {"threads", "--to=" + testDirectory.absolutePath().getAbsolutePath() + "/reports"};
            Path homeDir = testDirectory.directory().toPath();
            DiagnosticsReportCommand diagnosticsReportCommand =
                    new DiagnosticsReportCommand( homeDir, homeDir, outsideWorld );
            diagnosticsReportCommand.execute( args );
        }
        catch ( IncorrectUsage e )
        {
            if ( e.getMessage().equals( "Unknown classifier: threads" ) )
            {
                return; // If we get attach API is not available for example in some IBM jdk installs, ignore this test
            }
            throw e;
        }

        // Verify that we took a thread dump
        File reports = testDirectory.directory( "reports" );
        File[] files = reports.listFiles();
        assertThat( files, notNullValue() );
        assertThat( files.length, is( 1 ) );

        Path report = files[0].toPath();
        final URI uri = URI.create("jar:file:" + report.toUri().getPath());

        try ( FileSystem fs = FileSystems.newFileSystem( uri, Collections.emptyMap() ) )
        {
            String threadDump = new String( Files.readAllBytes( fs.getPath( "threaddump.txt" ) ) );
            assertThat( threadDump, containsString( DiagnosticsReportCommandIT.class.getCanonicalName() ) );
        }
    }

    private static long getPID()
    {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if ( processName != null && processName.length() > 0 )
        {
            try
            {
                return Long.parseLong( processName.split( "@" )[0] );
            }
            catch ( Exception ignored )
            {
            }
        }

        return 0;
    }
}
