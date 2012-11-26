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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.monitoring.runtime.instrumentation.AllocationRecorder;
import com.google.monitoring.runtime.instrumentation.Sampler;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.performance.domain.benchmark.Benchmark;

/**
 * This is the Main application that gets invoked in the subprocess JVM that we start
 * in order to do memory sampling.
 */
class SubprocessMemoryProfiler
{


    private static class MemoryProfiler implements Sampler
    {

        private AtomicBoolean isSampling = new AtomicBoolean( false );
        private MemoryProfilingReport report;

        @Override
        public void sampleAllocation( int count, String desc,
                                      Object newObj, long size )
        {
            if(isSampling.get())
            {
                report.recordSample(count, desc, newObj, size);
            }
        }

        public void startProfiling()
        {
            report = new MemoryProfilingReport();
            isSampling.set( true );
        }

        public void stopProfiling()
        {
            isSampling.set( false );
        }

        public MemoryProfilingReport createReport()
        {
            return report;
        }
    }

    private final MemoryProfiler profiler;

    public SubprocessMemoryProfiler()
    {
        profiler = new MemoryProfiler();
        AllocationRecorder.addSampler( profiler );
    }

    public static void main(String ... args)
    {
        if(args.length != 1)
        {
            terminateWithError("No operation class specified");
        }

        try {
            SubprocessMemoryProfiler runner = new SubprocessMemoryProfiler();

            MemoryProfilingReport report = runner.runAndReport( args[0] );

            report.serialize( System.out );

            System.out.print( "\0" );
            System.exit( 0 );
        } catch(Throwable e)
        {
            e.printStackTrace( System.err );
            System.exit( 1 );
        }
    }

    public MemoryProfilingReport runAndReport( String benchmarkClass ) throws Exception
    {
        Benchmark benchmark = createBenchmark( benchmarkClass );
        benchmark.setUp();
        try
        {
            try {
                profiler.startProfiling();
                benchmark.run();
            } finally {
                profiler.stopProfiling();
            }
        } finally {
            benchmark.tearDown();
        }

        return profiler.createReport();
    }

    private Benchmark createBenchmark( String className )
    {
        Object testCase = null;
        try
        {
            Class<?> testClass = ClassLoader.getSystemClassLoader().loadClass( className );

            System.err.println( testClass );

            testCase = testClass.newInstance();
        }
        catch ( Throwable e )
        {
            terminateWithError( e );
        }

        if(!(testCase instanceof Benchmark))
        {
            terminateWithError("Test class must be a subclass of Operation");
        }

        return (Benchmark)testCase;
    }



    private static void terminateWithError( Throwable e )
    {
        e.printStackTrace( System.err );

        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        e.printStackTrace( printWriter );
        terminateWithError(result.toString());
    }

    private static void terminateWithError( String msg )
    {
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> out = new HashMap<String, Object>();
            out.put( "error", msg );
            mapper.writeValue( System.out, out );
        }
        catch ( Throwable e )
        {
            e.printStackTrace( System.err );
        }
        System.exit( 1 );
    }
}
