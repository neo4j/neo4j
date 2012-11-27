package org.neo4j.bench.cases;

/**
 * Encompasses some simple or complex operation that can be performed, used for
 * memory profiling and for simple benchmarks.
 */
public interface Operation
{

    public void setUp();
    public void invoke();
    public void tearDown();

}
