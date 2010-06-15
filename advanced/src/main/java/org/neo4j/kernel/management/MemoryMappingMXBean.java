package org.neo4j.kernel.management;

public interface MemoryMappingMXBean
{
    final String NAME = "Memory Mapping";

    WindowPoolInfo[] getMemoryPools();
}
