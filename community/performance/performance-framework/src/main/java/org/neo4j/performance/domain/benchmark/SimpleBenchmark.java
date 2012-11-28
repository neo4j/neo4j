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
package org.neo4j.performance.domain.benchmark;

import static org.neo4j.performance.domain.Units.OPERATION;

import org.neo4j.performance.domain.Units;

/**
 * Used to do very simple benchmarks, where you implement a single method that does your work, and then
 * returns the number of operations that was performed. This class will take care of converting that
 * result into proper ops/sec output.
 */
public abstract class SimpleBenchmark extends BenchmarkAdapter
{

    private int numberOfOps;

    public SimpleBenchmark()
    {
        this(1000);
    }

    public SimpleBenchmark(int numberOfOperationsToRun)
    {
        numberOfOps = numberOfOperationsToRun;
    }

    @Override
    public BenchmarkResult run() throws Exception
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

        // TODO: Add tracking for eg. mean, average, max and min times
        return new BenchmarkResult( getName(),
                new BenchmarkResult.Metric( "result", opsPerUnit, OPERATION.per( Units.MILLISECOND ) ) );
    }

    public String getName()
    {
        return getClass().getName();
    }

    /**
     * Implement the operation you want to benchmark here.
     * @return
     */
    public abstract void runOperation() throws Exception;
}
