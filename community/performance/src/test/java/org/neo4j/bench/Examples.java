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
package org.neo4j.bench;

import static org.neo4j.bench.domain.Units.MILLISECOND;
import static org.neo4j.bench.domain.Units.OPERATION;
import static org.neo4j.bench.domain.Units.SECOND;

import org.neo4j.bench.cases.BenchmarkAdapter;
import org.neo4j.bench.cases.SimpleBenchmark;
import org.neo4j.bench.cases.concurrent.BenchWorker;
import org.neo4j.bench.cases.concurrent.ConcurrentBenchmark;
import org.neo4j.bench.cases.concurrent.WorkerContext;
import org.neo4j.bench.cases.concurrent.WorkerMetric;
import org.neo4j.bench.cases.memory.MemoryBenchmark;
import org.neo4j.bench.domain.CaseResult;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * This is just to show how you would add a main method to run your benchmark stand-alone.
 */
public class Examples
{

    /**
     * Dead-simple operations/second test. The {@link SimpleBenchmark} expects
     * you to implement a method that loops over and executes the operation you are benchmarking
     * a number of times, enough for you to feel that you should get meningful results.
     *
     * The parent class then takes care of timing and figuring out how many operations per second
     * where executed.
     */
    public static class SimpleWithMainMethod extends SimpleBenchmark
    {

        // setUp and tearDown are available for override if you want to do those things outside
        // of your benchmark code.

        @Override
        public long runOperations()
        {
            int numberOfOpsToDo = 10000;

            for(int i=0;i<numberOfOpsToDo;i++)
            {
                int a = 10000 * 10000;
            }

            return numberOfOpsToDo;
        }

        public static void main(String ... args)
        {
            BenchmarkRunner.runAndDumpReport( new SimpleWithMainMethod() );
        }
    }

    /**
     * If you want to report more than just a basic operations-per-second, you can use
     * a {@link BenchmarkAdapter} extending class to write a more complex benchmark.
     */
    public static class StandardBenchmark extends BenchmarkAdapter
    {

        // setUp and tearDown are available for override if you want to do those things outside
        // of your benchmark code.

        @Override
        public CaseResult run()
        {
            // Do benchmark work

            // Report result
            return new CaseResult( "MyBenchmark",
                    new CaseResult.Metric( "avg time",      53.0, MILLISECOND.per( OPERATION ) ),
                    new CaseResult.Metric( "peak time",    153.0, MILLISECOND.per( OPERATION ) ),
                    new CaseResult.Metric( "total time", 10253.0, MILLISECOND )
            );
        }
    }

    /**
     * This shows how you would write a test that tests concurrent load.
     */
    public static class ConcurrentWithMainMethod extends ConcurrentBenchmark
    {

        public ConcurrentWithMainMethod( )
        {
            super( 10 );
        }

        // Optional set-up method
        @Override
        public void setUp()
        {
            super.setUp();
            getWorkerContext().put( "something I want all workers to access", new Object() );
        }

        // Optional tear down method
        @Override
        public void tearDown()
        {
            // Do cleanup
            super.tearDown();
        }

        @Override
        public BenchWorker createWorker()
        {
            return new BenchWorker()
            {
                @Override
                public WorkerMetric[] doWork( WorkerContext ctx )
                {
                    // Do some benchmarking

                    // Then report the result for this worker
                    return new WorkerMetric[]
                    {
                        new WorkerMetric( WorkerMetric.TOTAL, "A result that is summed up for all workers", 3.0,
                                OPERATION.per( SECOND )),

                        new WorkerMetric( WorkerMetric.AVERAGE, "A result that is reported as the average reported",
                                3.0,
                                SECOND.per( OPERATION ))
                    };
                }
            };
        }

        public static void main(String ... args)
        {
            BenchmarkRunner.runAndDumpReport( new ConcurrentWithMainMethod() );
        }
    }

    public static class MemoryProfilingWithMainMethod extends MemoryBenchmark
    {

        @Override
        public void invoke()
        {
            // Some basic objects
            new Object();
            new String( "Another string!" );
            int [] intArray = new int[]{ 1,2,3 };

            // Allocations in a loop
            for(int i=10; i --> 0;)
            {
                new String( "A string!" + i);
            }
        }

        public static void main(String ... args)
        {
            BenchmarkRunner.runAndDumpReport( new MemoryProfilingWithMainMethod() );
        }
    }



    public static class ProfilingNeo extends MemoryBenchmark
    {

        private EmbeddedGraphDatabase neo;

        @Override
        public void setUp()
        {
            neo = new EmbeddedGraphDatabase( "/tmp/blah" );
        }

        @Override
        public void tearDown()
        {
            neo.shutdown();
//            try
//            {
//                FileUtils.deleteRecursively( new File( "/tmp/blah" ) );
//            }
//            catch ( IOException e )
//            {
//                throw new RuntimeException( e );
//            }
        }

        @Override
        public void invoke()
        {
            //neo.createNode();
        }

        public static void main(String ... args)
        {
            ProfilingNeo benchmark = new ProfilingNeo();
            BenchmarkRunner.runAndDumpReport( benchmark );
        }
    }
}
