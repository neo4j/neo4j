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
package org.neo4j.bench.cases;

import static org.neo4j.bench.domain.Units.OPERATION;

import org.neo4j.bench.domain.CaseResult;
import org.neo4j.bench.domain.Units;

/**
 * Used to do very simple benchmarks, where you implement a single method that does your work, and then
 * returns the number of operations that was performed. This class will take care of converting that
 * result into proper ops/sec output.
 */
public abstract class SimpleBenchmark extends BenchmarkAdapter
{
    @Override
    public CaseResult run()
    {
        long start = System.nanoTime();
        long ops = runOperations();
        long end = System.nanoTime();

        double delta = (end - start);

        // Convert to milliseconds
        delta = delta / (1000.0 * 1000.0);

        double opsPerUnit = (ops * 1.0) / delta;

        return new CaseResult( getName(),
                new CaseResult.Metric( "result", opsPerUnit, OPERATION.per( Units.MILLISECOND ) ) );
    }

    public String getName()
    {
        return getClass().getName();
    }

    /**
     * Implement your benchmark code here. It should perform an operation of your choice a number of times,
     * and return how many times it did it. Please ensure that it runs the operation you are testing enough
     * times to be meningful, eg. at least a few hundred milliseconds, or up to several hours if you need that
     * to get stable results.
     *
     * The test harness code will take care of tracking time and report the number of operations per millisecond.
     *
     * @return
     */
    public abstract long runOperations();
}
