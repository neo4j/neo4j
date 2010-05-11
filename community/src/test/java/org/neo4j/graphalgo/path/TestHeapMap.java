package org.neo4j.graphalgo.path;

import org.junit.Test;

public class TestHeapMap
{
    @Test
    public void heapMapImplementsAMinHeap()
    {
        HeapMap<Integer, Double> heapmap = new HeapMap<Integer, Double>();
        heapmap.put( 1, 10d );
        heapmap.put( 1, 10d );
    }
}
