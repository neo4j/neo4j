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
package org.neo4j.performance;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Date;

import org.neo4j.performance.domain.RunResult;
import org.neo4j.performance.domain.benchmark.Benchmark;
import org.neo4j.performance.domain.benchmark.BenchmarkLoader;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;
import org.neo4j.performance.domain.benchmark.BenchmarkRunner;
import org.neo4j.performance.domain.benchmark.RunnerProvider;

public class PerformanceProfiler implements BenchmarkRunner
{

    private final BenchmarkLoader caseLoader;
    private final String testedVersion;
    private final String buildURL;

    /**
     * Convienience method for running a single benchmark.
     * @param benchmark
     */
    public static void runAndDumpReport(Benchmark benchmark) throws IOException
    {
        try {
            BenchmarkResult result = new PerformanceProfiler().run( benchmark );
            result.createReport( "", new OutputStreamWriter( System.out ) );
        } catch(IOException e)
        {
            throw e;
        } catch(Exception e)
        {
            throw new RuntimeException( e );
        }
    }

    public static void runAndDumpReport(Benchmark benchmark, File output) throws IOException
    {
        if(output.exists())
        {
            output.delete();
        }

        FileWriter out = new FileWriter( output );
        try
        {
            BenchmarkResult result = new PerformanceProfiler().run( benchmark );

            result.createReport( "", out );
        }
        catch( IOException e)
        {
            throw e;
        }
        catch ( Exception e )
        {
            e.printStackTrace( new PrintWriter( out ) );
        }
        finally
        {
            out.close();
        }
    }

    public PerformanceProfiler()
    {
        this("Unknown version", "N/A");
    }

    public PerformanceProfiler( String testedVersion, String buildURL )
    {
        this.testedVersion = testedVersion;
        this.buildURL = buildURL;
        caseLoader = new BenchmarkLoader();
    }

    public BenchmarkResult run(Benchmark benchmark) throws Exception
    {
        if(benchmark instanceof RunnerProvider)
        {
            // Benchmark has a novelty runner that it wants to use
            return ((RunnerProvider)benchmark).getBenchmarkRunner().run(benchmark);
        } else
        {
            benchmark.setUp();
            try
            {
                return benchmark.run();
            }
            finally {
                benchmark.tearDown();
            }
        }
    }

    public RunResult runAllInPackage(String rootPackage) throws Exception
    {
        RunResult result = new RunResult( testedVersion, new Date( ), buildURL );
        for ( Benchmark benchmarkCase : caseLoader.loadBenchmarksIn( rootPackage ) )
        {
            result.addResult( run( benchmarkCase ) );
        }

        return result;
    }

}
