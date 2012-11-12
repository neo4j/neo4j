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
package org.neo4j.bench.cases.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.bench.cases.BenchmarkAdapter;
import org.neo4j.bench.domain.CaseResult;

/**
 * Helps you with a test harness for running concurrent load. Specify the number of concurrent workers,
 * and a worker method to be executed. You can also, of course, optionally override setUp and tearDown methods
 * to set up context that should be available to all workers. Make sure to call setUp and tearDown super methods
 * if you do override those.
 *
 * In order to give workers access to things that you set up and tear down, use the WorkerContext that is available
 * via getContext().
 *
 * Each worker is expected to time it's own operations and return a set of metrics, those metrics are then combined into
 * totals.
 */
public abstract class ConcurrentBenchmark extends BenchmarkAdapter
{
    private final int numberOfWorkers;
    private WorkerContext workerContext;

    public ConcurrentBenchmark( int numberOfWorkers )
    {
        this.numberOfWorkers = numberOfWorkers;
    }


    public abstract BenchWorker createWorker();


    @Override
    public CaseResult run()
    {
        ExecutorService executorService = Executors.newFixedThreadPool( numberOfWorkers );
        try
        {
            Collection<CallableBenchWorker> workers = new ArrayList<CallableBenchWorker>();
            Map<String, WorkerMetric> aggregatedResults = new HashMap<String, WorkerMetric>();

            // Create workers
            for(int i=0;i<numberOfWorkers;i++)
            {
                workers.add( new CallableBenchWorker(createWorker(), getWorkerContext()) );
            }

            // Run
            for ( Future<Collection<WorkerMetric>> future : executorService.invokeAll( workers ) )
            {
                for ( WorkerMetric workerMetric : future.get() )
                {
                    if(!aggregatedResults.containsKey(
                            workerMetric.getName() ))
                    {
                        aggregatedResults.put( workerMetric.getName(), workerMetric );
                    } else {
                        aggregatedResults.put( workerMetric.getName(),
                                aggregatedResults.get( workerMetric.getName() ).aggregateWith( workerMetric ));
                    }
                }
            }


            // Create output CaseResult
            CaseResult.Metric[] metrics = aggregatedResults.values().toArray(
                    new CaseResult.Metric[aggregatedResults.size()]);

            return new CaseResult( getClass().getName(), metrics );

        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Override
    public void setUp()
    {
        super.setUp();
        workerContext = new WorkerContext();
    }

    @Override
    public void tearDown()
    {
        workerContext = null;
        super.tearDown();
    }

    public WorkerContext getWorkerContext()
    {
        return workerContext;
    }

}
