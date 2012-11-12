package org.neo4j.bench.cases.memory;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.neo4j.bench.domain.CaseResult;

public class TestMemoryBenchmark
{

    public static class AllocateTwoObjectsBenchmark extends MemoryBenchmark
    {
        @Override
        public void invoke()
        {
            new Object();
            new Object();
        }
    };

    public static class AllocateNothingBenchmark extends MemoryBenchmark
    {
        @Override
        public void invoke()
        {
        }
    };

    @Test
    public void shouldReportCorrectCountsAndByteSizes() throws Exception
    {
        // Given
        MemoryBenchmark benchmark = new AllocateTwoObjectsBenchmark();

        // When
        CaseResult result = benchmark.run();

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
        CaseResult result = benchmark.run();

        // Then
        assertThat(result.containsMetric( "memory usage" ), is(true));
        assertThat(result.getMetric( "memory usage" ).getValue(), is(0.0));

        assertThat(result.containsMetric( "objects allocated" ), is(true));
        assertThat(result.getMetric( "objects allocated" ).getValue(), is(0.0));
    }

}
