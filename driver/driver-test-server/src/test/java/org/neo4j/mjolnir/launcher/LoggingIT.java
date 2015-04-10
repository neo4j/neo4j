/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.mjolnir.launcher;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.mjolnir.launcher.JavaRun.buildProcess;
import static org.neo4j.mjolnir.launcher.util.LauncherMatchers.assertEventually;
import static org.neo4j.mjolnir.launcher.util.LauncherMatchers.serverListensTo;

public class LoggingIT
{
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    private Process proc;

    @Test
    public void outputsVerySpecificInformationToStdOutByDefault() throws Throwable
    {
        // Given
        String workDir = tempDir.getRoot().getAbsolutePath();

        // When
        String output = runAndStop( workDir );

        // Then
        assertThat( output, equalTo(
                "Neo4j started\n" +
                "Neo4j stopped\n") );
    }

    private String runAndStop( String workDir ) throws IOException, InterruptedException
    {
        File stdOutFile = tempDir.newFile();
        File stdErrFile = tempDir.newFile();

        proc = buildProcess( Launcher.class,
                "-c", "dbms.datadir=" + workDir,
                "-c", "dbms.mjolnir.address=:7999" )
            .redirectOutput( stdOutFile )
            .redirectError( stdErrFile )
            .start();

        assertEventually( "Should listen to configured port", serverListensTo( "http://localhost:7999" ) );

        proc.destroy();
        proc.waitFor();

        // Read the output printed to stdout and stderr
        final StringBuilder sb = new StringBuilder();
        for ( String line : Files.readAllLines( stdErrFile.toPath(), Charset.defaultCharset() ) )
        {
            sb.append( line ).append( "\n" );
        }
        for ( String line : Files.readAllLines( stdOutFile.toPath(), Charset.defaultCharset() ) )
        {
            sb.append( line ).append( "\n" );
        }

        return sb.toString();
    }

    @After
    public void cleanup()
    {
        if(proc != null)
        {
            proc.destroy();
        }
    }
}
