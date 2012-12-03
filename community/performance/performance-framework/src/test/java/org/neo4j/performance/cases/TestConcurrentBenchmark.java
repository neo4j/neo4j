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
package org.neo4j.performance.cases;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.performance.domain.Units.MILLISECOND;
import static org.neo4j.performance.domain.Units.OPERATION;
import static org.neo4j.performance.domain.Units.SECOND;

import org.junit.Test;
import org.neo4j.performance.domain.benchmark.BenchmarkResult;
import org.neo4j.performance.domain.benchmark.Benchmark;
import org.neo4j.performance.domain.benchmark.concurrent.BenchmarkWorker;
import org.neo4j.performance.domain.benchmark.concurrent.ConcurrentBenchmark;
import org.neo4j.performance.domain.benchmark.concurrent.WorkerMetric;

public class TestConcurrentBenchmark
{

    @Test
    public void shouldAggregateResultsCorrectly() throws Exception
    {
        // Given
        Benchmark bench = new ConcurrentBenchmark(8)
        {
            @Override
            public BenchmarkWorker createWorker()
            {
                return new BenchmarkWorker()
                {
                    @Override
                    public WorkerMetric[] doWork( )
                    {
                        return new WorkerMetric[] {
                            new WorkerMetric( WorkerMetric.TOTAL, "The metric", 1.0, OPERATION.per( SECOND ) ),
                            new WorkerMetric( WorkerMetric.AVERAGE, "Avg metric", 53.0, MILLISECOND.per( OPERATION ) )
                        };
                    }
                };
            }
        };

        // When
        BenchmarkResult result = bench.run();

        // Then
        assertThat( result.getMetric( "The metric" ).getValue(), is(8.0));
        assertThat( result.getMetric( "Avg metric" ).getValue(), is(53.0));
    }
}
