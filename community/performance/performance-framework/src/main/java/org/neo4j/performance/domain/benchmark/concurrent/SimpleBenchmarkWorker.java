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
package org.neo4j.performance.domain.benchmark.concurrent;

import static org.neo4j.performance.domain.Units.MILLISECOND;
import static org.neo4j.performance.domain.Units.OPERATION;

public abstract class SimpleBenchmarkWorker implements BenchmarkWorker
{

    private final int numberOfOps;

    public SimpleBenchmarkWorker()
    {
        this(1000);
    }

    public SimpleBenchmarkWorker(int numberOfOperationsToRun)
    {
        numberOfOps = numberOfOperationsToRun;
    }

    /**
     * Run a single operation
     */
    public abstract void runOperation();

    @Override
    public WorkerMetric[] doWork()
    {
        int opsLeft = numberOfOps;
        long start = System.nanoTime();
        while(opsLeft --> 0)
        {
            runOperation();
        }
        double delta = (System.nanoTime() - start);

        // Convert to milliseconds
        delta = delta / (1000.0 * 1000.0);

        double opsPerUnit = numberOfOps / delta;

        // TOOD: Look at simplebenchmark, these are almost the same, look for common ground to implement eg
        // max,min,avg,mean,total in a uniform way.
        return new WorkerMetric[]{
                new WorkerMetric( WorkerMetric.TOTAL, "Ops per sec", opsPerUnit, OPERATION.per( MILLISECOND ) )};
    }
}
