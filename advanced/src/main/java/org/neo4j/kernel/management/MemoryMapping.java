package org.neo4j.kernel.management;

public interface MemoryMapping
{
    final String NAME = "Memory Mapping";

    WindowPoolInfo[] getMemoryPools();
}
