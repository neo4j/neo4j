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

import org.neo4j.performance.domain.benchmark.BenchmarkAdapter;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;
import org.neo4j.performance.domain.benchmark.BenchmarkRunner;
import org.neo4j.performance.domain.benchmark.RunnerProvider;

public abstract class MemoryBenchmark extends BenchmarkAdapter implements RunnerProvider
{

    public abstract void operationToProfile();

    @Override
    public BenchmarkRunner getBenchmarkRunner()
    {
        return new MemoryBenchmarkRunner();
    }

    @Override
    public BenchmarkResult run()
    {
        operationToProfile();

        // The memory profiling runner will ignore the result we return here
        return null;
    }
}
