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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.bench.domain.Units.MILLISECOND;
import static org.neo4j.bench.domain.Units.OPERATION;
import static org.neo4j.bench.domain.Units.SECOND;

import org.junit.Test;
import org.neo4j.bench.cases.concurrent.BenchWorker;
import org.neo4j.bench.cases.concurrent.ConcurrentBenchmark;
import org.neo4j.bench.cases.concurrent.WorkerContext;
import org.neo4j.bench.cases.concurrent.WorkerMetric;
import org.neo4j.bench.domain.CaseResult;

public class TestConcurrentBenchmark
{

    @Test
    public void shouldAggregateResultsCorrectly() throws Exception
    {
        // Given
        Benchmark bench = new ConcurrentBenchmark(8)
        {
            @Override
            public BenchWorker createWorker()
            {
                return new BenchWorker()
                {
                    @Override
                    public WorkerMetric[] doWork( WorkerContext ctx )
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
        CaseResult result = bench.run();

        // Then
        assertThat( result.getMetric( "The metric" ).getValue(), is(8.0));
        assertThat( result.getMetric( "Avg metric" ).getValue(), is(53.0));
    }
}
