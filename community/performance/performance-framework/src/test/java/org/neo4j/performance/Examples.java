/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.performance.domain.Units.MILLISECOND;
import static org.neo4j.performance.domain.Units.OPERATION;
import static org.neo4j.performance.domain.Units.SECOND;

import java.io.IOException;

import org.neo4j.performance.domain.benchmark.BenchmarkAdapter;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;
import org.neo4j.performance.domain.benchmark.SimpleBenchmark;
import org.neo4j.performance.domain.benchmark.concurrent.BenchmarkWorker;
import org.neo4j.performance.domain.benchmark.concurrent.ConcurrentBenchmark;
import org.neo4j.performance.domain.benchmark.concurrent.SimpleBenchmarkWorker;
import org.neo4j.performance.domain.benchmark.concurrent.WorkerMetric;

/**
 * A set of example benchmarks. Any class that implements the Benchmark interface will
 * get picked up by the dashboard, where regression tracking and run history is kept.
 */
public class Examples
{

    /**
     * Dead-simple operations/second test. The {@link SimpleBenchmark} expects
     * you to implement a method that executes the operation you want to benchmark.
     *
     * The parent class then takes care of iterating over that operation, timing it
     * and figuring out how many operations per second where executed.
     *
     * Right now, the number of iterations is hard coded, and you can override it by
     * passing a parameter to the parent constructor. Later on, the idea is that simple
     * benchmark will be smarter, running the operation until the standard deviation is within
     * some reasonable limit.
     */
    public static class Simple extends SimpleBenchmark
    {

        // setUp and tearDown are available for override if you want to do those things outside
        // of your benchmark code.

        @Override
        public void runOperation()
        {
            int a = 10000 * 10000;
        }

        // A simple main method, to allow you to run this test locally while debugging or
        // doing performance improvements.
        public static void main(String ... args) throws IOException
        {
            PerformanceProfiler.runAndDumpReport( new Simple() );
        }
    }

    // If you want to report more than just a basic operations-per-second, you can use
    // a {@link BenchmarkAdapter} extending class to write a more complex benchmark.

    public static class StandardBenchmark extends BenchmarkAdapter
    {

        // setUp and tearDown are available for override if you want to do those things outside
        // of your benchmark code.

        @Override
        public BenchmarkResult run()
        {
            // Do benchmark work

            // Report result
            return new BenchmarkResult( "MyBenchmark",
                    new BenchmarkResult.Metric( "avg time",      53.0, MILLISECOND.per( OPERATION ) ),
                    new BenchmarkResult.Metric( "peak time",    153.0, MILLISECOND.per( OPERATION ) ),
                    new BenchmarkResult.Metric( "total time", 10253.0, MILLISECOND )
            );
        }

        // Main method to run locally

        public static void main(String ... args) throws IOException
        {
            PerformanceProfiler.runAndDumpReport( new Simple() );
        }
    }

    // An example concurrent benchmark

    public static class SimpleConcurrent extends ConcurrentBenchmark
    {

        public SimpleConcurrent()
        {
            // Specify the number of concurrent workers we want
            super( 10 );
        }

        @Override
        public BenchmarkWorker createWorker()
        {
            return new SimpleBenchmarkWorker()
            {
                // You can call the parent constructor here to hard-code how many times the operation
                // should be invoked, just like with the Simple benchmark

                @Override
                public void runOperation()
                {
                    int i = 1 + 1;
                }
            };
        }

        // And a main method again

        public static void main(String ... args) throws IOException
        {
            PerformanceProfiler.runAndDumpReport( new SimpleConcurrent() );
        }
    }

    // This shows how you would write a test that tests concurrent load, where we use the raw BenchmarkWorker
    // interface, rather than the SimpleBenchmarkWorker. This gives us the power to define custom metrics, but
    // it means we need to write our own timing code.

    public static class ConcurrentWithMainMethod extends ConcurrentBenchmark
    {

        private int sharedValueAcrossWorkers;

        public ConcurrentWithMainMethod( )
        {
            // Number of concurrent workers
            super( 10 );
        }

        // Optional set-up method
        @Override
        public void setUp()
        {
            sharedValueAcrossWorkers = 1337;
        }

        // Optional tear down method
        @Override
        public void tearDown()
        {
        }

        @Override
        public BenchmarkWorker createWorker()
        {
            return new BenchmarkWorker()
            {
                @Override
                public WorkerMetric[] doWork( )
                {
                    // Do some benchmarking


                    // Then report the result for this worker
                    return new WorkerMetric[]
                    {
                        new WorkerMetric( WorkerMetric.TOTAL, "A result that is summed up for all workers", 3.0,
                                OPERATION.per( SECOND )),

                        new WorkerMetric( WorkerMetric.AVERAGE, "A result that is reported as the average reported",
                                sharedValueAcrossWorkers * 12,
                                SECOND.per( OPERATION ))
                    };
                }
            };
        }

        // And, main method again

        public static void main(String ... args) throws IOException
        {
            PerformanceProfiler.runAndDumpReport( new ConcurrentWithMainMethod() );
        }
    }
}
