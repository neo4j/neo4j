package org.neo4j.bench.cases.memory;

import org.neo4j.bench.cases.BenchmarkAdapter;
import org.neo4j.bench.cases.Operation;
import org.neo4j.bench.domain.CaseResult;

public abstract class MemoryBenchmark extends BenchmarkAdapter implements Operation
{
    @Override
    public CaseResult run()
    {
        MemoryProfiler profiler = new MemoryProfiler();

        MemoryProfilingReport report = profiler.run(getClass());

        return new MemoryCaseResult( getName(), report);
    }

    public String getName()
    {
        return getClass().getName();
    }


}
