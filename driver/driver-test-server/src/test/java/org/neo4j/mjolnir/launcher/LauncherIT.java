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
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Supplier;
import org.neo4j.mjolnir.launcher.util.LauncherMatchers;

import static org.junit.Assert.assertTrue;
import static org.neo4j.mjolnir.launcher.JavaRun.buildProcess;
import static org.neo4j.mjolnir.launcher.JavaRun.exec;

public class LauncherIT
{
    @Rule public TemporaryFolder tempDir = new TemporaryFolder();
    private Process proc;

    @Test
    public void shouldAcceptConfigViaCommandLine() throws Throwable
    {
        // Given
        String workDir = tempDir.getRoot().getAbsolutePath();

        // When
        proc = exec( Launcher.class, "-c", "dbms.datadir=" + workDir);

        // Then
        LauncherMatchers.assertEventually( "Should've used provided directory", new Supplier<Boolean>()
        {
            @Override
            public Boolean get()
            {
                return new File( tempDir.getRoot(), "default.graphdb" ).exists();
            }
        });
    }

    @Test
    public void shouldControlHostAndPort() throws Throwable
    {
        // Given
        String workDir = tempDir.getRoot().getAbsolutePath();

        // When
        proc = exec( Launcher.class,
                "-c", "dbms.datadir=" + workDir,
                "-c", "dbms.mjolnir.address=:7999");

        String address = "http://localhost:7999";

        // Then
        LauncherMatchers.assertEventually(
                "Should listen to configured port",
                LauncherMatchers.serverListensTo( address ) );
    }

    @Test
    public void shouldDumpConfigOptions() throws Throwable
    {
        // Given
        File stdOutFile = tempDir.newFile();
        File stdErrFile = tempDir.newFile();

        // When
        proc = buildProcess( Launcher.class, "--config-options" )
            .redirectOutput(stdOutFile)
            .redirectError( stdErrFile )
            .start();

        waitFor( proc, 10, TimeUnit.SECONDS );

        // Then the process should exit on its own, and the output should contain config options
        String output = collectOutput( stdOutFile, stdErrFile );
        assertTrue( output.contains( HandAssembledNeo4j.Settings.data_dir.name() ) );
    }

    private String collectOutput( File stdOutFile, File stdErrFile ) throws IOException
    {
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

    private static void waitFor( Process proc, final int time, final TimeUnit unit ) throws InterruptedException
    {
        final Thread mainThread = Thread.currentThread();

        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    unit.sleep( time );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                mainThread.interrupt();
            }
        } ).start();

        proc.waitFor();
    }
}