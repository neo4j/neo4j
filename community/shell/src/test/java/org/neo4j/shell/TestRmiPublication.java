/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.junit.Test;
import org.neo4j.test.ProcessStreamHandler;

import static java.lang.Runtime.*;
import static java.lang.System.*;
import static org.junit.Assert.*;
import static org.neo4j.test.TargetDirectory.*;

public class TestRmiPublication
{
    @Test
    public void jvmShouldDieEvenIfWeLeaveSamveJvmClientIsLeftHanging() throws Exception
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
        String dir = forTest( getClass() ).directory( "client", true ).getAbsolutePath();
        return waitForExit( getRuntime().exec( new String[] { "java", "-cp", getProperty( "java.class.path" ),
                mainClass.getName(), dir } ), 20 );
    }

    private int waitForExit( Process process, int maxSeconds ) throws InterruptedException
    {
        long endTime = System.currentTimeMillis() + maxSeconds*1000;
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
            throw new RuntimeException( "Process didn't exit on its own" );
        }
        finally
        {
            streamHandler.cancel();
        }
    }
}
