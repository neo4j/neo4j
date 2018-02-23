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
package org.neo4j.shell;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.util.Properties;
import javax.annotation.Resource;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.getProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( TestDirectoryExtension.class )
public class RmiPublicationIT
{
    @Resource
    public TestDirectory testDirectory;

    public static File createDefaultConfigFile( File path ) throws IOException
    {
        File configFile = new File( path, Config.DEFAULT_CONFIG_FILE_NAME );
        Properties config = new Properties();
        config.setProperty( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        try ( Writer writer = new FileWriter( configFile ) )
        {
            config.store( writer, "" );
        }
        return configFile;
    }

    @Test
    public void jvmShouldDieEvenIfWeLeaveSameJvmClientIsLeftHanging() throws Exception
    {
        assertEquals( 0, spawnJvm( DontShutdownClient.class, "client" ) );
    }

    @Test
    public void jvmShouldDieEvenIfLocalServerIsLeftHanging() throws Exception
    {
        assertEquals( 0, spawnJvm( DontShutdownLocalServer.class, "server" ) );
    }

    private int spawnJvm( Class<?> mainClass, String name ) throws Exception
    {
        String dir = testDirectory.directory( name ).getAbsolutePath();
        return waitForExit( getRuntime()
                .exec( new String[]{"java", "-cp", getProperty( "java.class.path" ), "-Djava.awt.headless=true",
                        mainClass.getName(), dir} ), 120 );
    }

    private int waitForExit( Process process, int maxSeconds ) throws InterruptedException
    {
        try
        {
            long endTime = System.currentTimeMillis() + maxSeconds * 1000;
            ProcessStreamHandler streamHandler = new ProcessStreamHandler( process, false );
            streamHandler.launch();
            try
            {
                while ( System.currentTimeMillis() < endTime )
                {
                    try
                    {
                        return process.exitValue();
                    }
                    catch ( IllegalThreadStateException e )
                    {   // OK, not exited yet
                        Thread.sleep( 100 );
                    }
                }

                tempHackToGetThreadDump( process );

                throw new RuntimeException( "Process didn't exit on its own." );
            }
            finally
            {
                streamHandler.cancel();
            }
        }
        finally
        {
            process.destroy();
        }
    }

    private void tempHackToGetThreadDump( Process process )
    {
        try
        {
            Field pidField = process.getClass().getDeclaredField( "pid" );
            pidField.setAccessible( true );
            int pid = (int) pidField.get( process );

            ProcessBuilder processBuilder = new ProcessBuilder( "/bin/sh", "-c", "kill -3 " + pid );
            processBuilder.redirectErrorStream( true );
            Process dumpProc = processBuilder.start();
            ProcessStreamHandler streamHandler = new ProcessStreamHandler( dumpProc, false );
            streamHandler.launch();
            try
            {
                process.waitFor();
            }
            finally
            {
                streamHandler.cancel();
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }
    }
}
