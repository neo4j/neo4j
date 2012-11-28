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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import org.junit.Test;
import org.neo4j.performance.domain.benchmark.Benchmark;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;

public class MemoryBenchmarkRunnerIT
{

    public static class AllocateTwoObjectsBenchmark extends MemoryBenchmark
    {
        @Override
        public void operationToProfile()
        {
            new Object();
            new Object();
        }
    };

    public static class AllocateNothingBenchmark extends MemoryBenchmark
    {
        @Override
        public void operationToProfile()
        {
        }
    };

    @Test
    public void shouldReportCorrectCountsAndByteSizes() throws Exception
    {
        // Given
        MemoryBenchmark benchmark = new AllocateTwoObjectsBenchmark();

        // When
        BenchmarkResult result = run( benchmark );

        // Then
        assertThat(result.containsMetric( "memory usage" ), is(true));
        assertThat(result.getMetric( "memory usage" ).getValue(), is(32.0));

        assertThat(result.containsMetric( "objects allocated" ), is(true));
        assertThat(result.getMetric( "objects allocated" ).getValue(), is(2.0));
    }

    @Test
    public void shouldReportNothingAllocatedIfNothingIsAllocated() throws Exception
    {
        // Given
        MemoryBenchmark benchmark = new AllocateNothingBenchmark();

        // When
        BenchmarkResult result = run( benchmark );

        // Then
        assertThat(result.containsMetric( "memory usage" ), is(true));
        assertThat(result.getMetric( "memory usage" ).getValue(), is(0.0));

        assertThat(result.containsMetric( "objects allocated" ), is(true));
        assertThat(result.getMetric( "objects allocated" ).getValue(), is(0.0));
    }


    private BenchmarkResult run(Benchmark benchmark)
    {
        MemoryBenchmarkRunner runner = new MemoryBenchmarkRunner();
        return runner.run( benchmark );
    }

}
