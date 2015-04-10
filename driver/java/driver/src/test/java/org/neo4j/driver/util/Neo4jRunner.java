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
package org.neo4j.driver.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.neo4j.Neo4j;
import org.neo4j.driver.Session;

import static java.io.File.separator;

import static junit.framework.TestCase.assertFalse;

/**
 * This class wraps the neo4j stand-alone jar in some code to help pulling it in from a remote URL and then launching
 * it in a separate process.
 */
public class Neo4jRunner
{
    public static final String DEFAULT_HTTP_URL = "http://localhost:7687";
    public static final String DEFAULT_URL = "neo4j://localhost:7687";

    private static Neo4jRunner globalInstance;

    private final File jarFile = new File( System.getProperty( "neo4j.jarTargetFile", "./target/neo4j.jar" ) );
    private final File dataDir = new File( System.getProperty( "neo4j.datadir", "./target/neo4j.data" ) );
    private Process process;

    public static void main( String... args ) throws Exception
    {
        Neo4jRunner neo4jRunner = new Neo4jRunner();
        neo4jRunner.startServer();
        neo4jRunner.stopServer();
    }

    /** Globall runner controlling a single server, used to avoid having to restart the server between tests */
    public static Neo4jRunner getOrCreateGlobalServer() throws IOException, InterruptedException
    {
        if ( globalInstance == null )
        {
            globalInstance = new Neo4jRunner();
            globalInstance.startServer();
        }
        return globalInstance;
    }

    // Look for the specified file in this directory and parent directories recursively
    private File findUpwards( String pathname, int levels )
    {
        File file = new File( pathname );
        if ( file.exists() )
        {
            return file;
        }
        else if ( levels == 0 )
        {
            return null;
        }
        else
        {
            return findUpwards( ".." + separator + pathname, levels - 1 );
        }
    }

    public Neo4jRunner() throws IOException
    {
        String artifactUrl = System.getenv( "neo4j.artifactUrl" );
        if ( artifactUrl == null )
        {
            File artifactFile = findUpwards(
                    "driver/driver-test-server/target/neo4j-driver-test-server-2.3-SNAPSHOT.jar", 3 );
            if ( artifactFile == null )
            {
                throw new FileNotFoundException( "Cannot locate launcher jar" );
            }
            artifactUrl = artifactFile.toURI().toURL().toString();
        }
        if ( jarFile.exists() && jarFile.length() == 0 )
        {
            jarFile.delete();
        }
        if ( !jarFile.exists() )
        {
            jarFile.getParentFile().mkdirs();
            System.out.println( "Copying: " + artifactUrl + " -> " + jarFile );
            streamFileTo( artifactUrl, jarFile );
        }
    }

    public void startServer() throws IOException, InterruptedException
    {
        assertFalse( "A server instance is already running", serverResponds() );

        FileTools.deleteRecursively( dataDir );

        String path = System.getProperty( "java.home" ) + separator + "bin" + separator + "java";
        process = new ProcessBuilder()
                .inheritIO()
                .command( path, "-jar", jarFile.getAbsolutePath(),
                        "-c", "dbms.datadir=" + dataDir.getAbsolutePath() ).start();

        // Add a shutdown hook to try and avoid leaking processes. This won't guard against kill -9 stops, but will
        // cover any other exit types.
        stopOnExit( process );

        awaitServerResponds();
    }

    public void clearData()
    {
        // Note - this hangs for extended periods some times, because there are tests that leave sessions running.
        // Thus, we need to wait for open sessions and transactions to time out before this will go through.
        // This could be helped by an extension in the future.
        try ( Session session = Neo4j.session( "neo4j://localhost" ) )
        {
            session.run( "MATCH (n) OPTIONAL MATCH (n)-[r]->() DELETE r,n" );
        }
    }

    public void stopServer() throws InterruptedException
    {
        process.destroy();
        waitFor( process, 5, TimeUnit.SECONDS );
        process.destroy();
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

    private void awaitServerResponds() throws IOException, InterruptedException
    {
        long timeout = System.currentTimeMillis() + 1000 * 30;
        for (; ; )
        {
            if ( serverResponds() )
            {
                return;
            }
            else
            {
                Thread.sleep( 100 );
            }

            // Make sure process still is alive
            try
            {
                int terminationCode = process.exitValue();
                throw new RuntimeException( "ERROR: Neo4j process died, exit code " + terminationCode );
            }
            catch ( IllegalThreadStateException e )
            {
                // process is still alive - ok
            }

            if ( System.currentTimeMillis() > timeout )
            {
                throw new RuntimeException( "Waited for 30 seconds for server to respond to HTTP calls, " +
                                            "but no response, timing out to avoid blocking forever." );
            }
        }
    }

    private boolean serverResponds() throws IOException, InterruptedException
    {
        try
        {
            URI.create( DEFAULT_HTTP_URL ).toURL().openStream().close();
            return true;
        }
        catch ( FileNotFoundException e )
        {
            // ok, 404, which is fine for now
            return true;
        }
        catch ( ConnectException e )
        {
            return false;
        }
    }

    /** To allow retrieving a runnable neo4j jar from the international webbernets, we have this */
    private static void streamFileTo( String url, File target ) throws IOException
    {
        try ( FileOutputStream out = new FileOutputStream( target );
              InputStream in = new URL( url ).openStream() )
        {
            byte[] buffer = new byte[1024];
            int read = in.read( buffer );
            while ( read != -1 )
            {
                if ( read > 0 )
                {
                    out.write( buffer, 0, read );
                }

                read = in.read( buffer );
            }
        }
    }

    private static void stopOnExit( final Process process )
    {
        Runtime.getRuntime().addShutdownHook( new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                process.destroy();
            }
        } ) );
    }

    public String url()
    {
        return DEFAULT_URL;
    }
}
