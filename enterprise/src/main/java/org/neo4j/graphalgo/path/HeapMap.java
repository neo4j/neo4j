package org.neo4j.graphalgo.path;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

class HeapMap<K, V>
{
    /*
     * This implementation is meant to be short, not efficient
     * these are some notes on how to make it more efficient:
     * o Use the same data structure for the mapping and the heap
     * o Only "sort" (to maintain the heap property) when pop is invoked
     *   the PriorityQueue used at the moment "sort" when it is modified,
     *   which means that our put method "sorts" the PriorityQueue twice.
     */
    private final Map<K, V> map;
    private final PriorityQueue<K> heap;

    public HeapMap()
    {
        map = new HashMap<K, V>();
        heap = new PriorityQueue<K>( 11, new Comparator<K>()
        {
            public int compare( K o1, K o2 )
            {
                return ( (Comparable) map.get( o1 ) ).compareTo( map.get( o2 ) );
            }
        } );
    }

    // The method that we want to add for HeapMap

    Map.Entry<K, V> pop()
    {
        K key = heap.poll();
        return new AbstractMap.SimpleEntry<K, V>( key, map.remove( key ) );
    }

    // The methods of Map that we need

    V get( Object key )
    {
        return map.get( key );
    }

    V put( K key, V value )
    {
        V result = map.put( key, value );
        if ( result != null )
        {
            heap.remove( key );
        }
        heap.add( key );
        return result;
    }
}
