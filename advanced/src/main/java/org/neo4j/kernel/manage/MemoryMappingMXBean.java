package org.neo4j.kernel.manage;

public interface MemoryMappingMXBean
{
    final String NAME = "Memory Mapping";

    WindowPoolInfo[] getMemoryPools();
}
