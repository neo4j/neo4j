/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.performance.domain.benchmark.memory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.performance.domain.benchmark.Benchmark;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;
import org.neo4j.performance.domain.benchmark.BenchmarkRunner;
import org.neo4j.performance.util.IOUtils;

public class MemoryBenchmarkRunner implements BenchmarkRunner
{

    @Override
    public BenchmarkResult run( Benchmark benchmark )
    {
        try {

            String memoryAgentJar = memoryAgentJar();
            Process proc = start( "java",
                    "-javaagent:" + memoryAgentJar,
                    "-Xmx2G",
                    //"-Xbootclasspath/a:" + createBootstrapClasspath( memoryAgentJar ),
                    "-cp", createClasspath( memoryAgentJar ),
                    SubprocessMemoryProfiler.class.getName(),
                    benchmark.getClass().getName() );

            InputStream source = proc.getInputStream();
            InputStream err = proc.getErrorStream();

            ByteOutputStream outBytes = new ByteOutputStream( 500000 );
            BufferedOutputStream out = new BufferedOutputStream( outBytes, 128 );

            outerloop: while(true)
            {
                // Read errors
                pipeAvailableChars( err, System.err );

                // Read stdin
                if ( pipeAvailableChars( source, out ) )
                {
                    break outerloop;
                }

                try
                {
                    int exitCode = proc.exitValue();
                    if(exitCode != 0)
                    {
                        pipeAvailableChars( err, System.err );
                        throw new RuntimeException( "Memory profiler failed, memory profiling JVM exited with " +
                                exitCode + " exit code. See system err output (probably console) for details." );
                    } else {
                        pipeAvailableChars( err, System.err );
                        pipeAvailableChars( source, out );
                        break outerloop;
                    }
                } catch(IllegalThreadStateException e)
                {

                }

                Thread.sleep( 10 );
            }

            out.flush();

            return new MemoryBenchmarkResult(
                    benchmark.getName(),
                    MemoryProfilingReport.deserialize( outBytes.getBytes() ) );

        } catch( RuntimeException e)
        {
            throw e;
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    private String createClasspath( String memoryAgentJar )
    {
        Collection<String> newCP = new ArrayList<String>();
        for ( String pathEntry : System.getProperty( "java.class.path" ).split( File.pathSeparator ) )
        {
            newCP.add( pathEntry );
        }

        return StringUtils.join(newCP, File.pathSeparator);
    }

    private boolean pipeAvailableChars( InputStream source, OutputStream out ) throws IOException
    {
        int available = source.available();
        if ( available != 0 )
        {
            byte[] data = new byte[available /*- ( available % 2 )*/];
            source.read( data );
            ByteBuffer chars = ByteBuffer.wrap( data );
            while ( chars.hasRemaining() )
            {
                char c = (char) chars.get();
                if ( c == '\0')
                {
                    return true;
                } else
                {
                    out.write( c );
                }
            }
        }
        return false;
    }

    private String memoryAgentJar(  ) throws IOException
    {
        File allocationJarTarget = new File( "target/allocation.jar" );
        if(!allocationJarTarget.exists())
        {
            InputStream stream = this.getClass().getClassLoader().
                    getResourceAsStream( "allocation.jar" );
            IOUtils.writeToFile(stream, allocationJarTarget );
        }

        return allocationJarTarget.getAbsolutePath();
    }

    private Process start( String... args )
    {
        ProcessBuilder builder = new ProcessBuilder( args );
        try
        {
            return builder.start();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to start sub process", e );
        }
    }
}
