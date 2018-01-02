/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl.shortestpath;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphalgo.impl.util.FibonacciHeap;
import org.neo4j.graphdb.Node;

/**
 * Implementation of {@link DijkstraPriorityQueue} using a {@link FibonacciHeap}
 * @param <CostType>
 *            The datatype the path weights are represented by.
 */
public class DijkstraPriorityQueueFibonacciImpl<CostType> implements
    DijkstraPriorityQueue<CostType>
{
    /**
     * Data structure used for the internal priority heap
     */
    protected class HeapObject
    {
        private Node node;
        private CostType cost;

        public HeapObject( Node node, CostType cost )
        {
            this.node = node;
            this.cost = cost;
        }

        public CostType getCost()
        {
            return cost;
        }

        public Node getNode()
        {
            return node;
        }

        /*
         * Equals is only defined from the stored node, so we can use it to find
         * entries in the queue
         */
        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj ) return true;
            if ( obj == null ) return false;
            if ( getClass() != obj.getClass() ) return false;
            final HeapObject other = (HeapObject) obj;
            if ( node == null )
            {
                if ( other.node != null ) return false;
            }
            else if ( !node.equals( other.node ) ) return false;
            return true;
        }

        @Override
        public int hashCode()
        {
            return node == null ? 23 : 14 ^ node.hashCode();
        }
    }

    Map<Node,FibonacciHeap<HeapObject>.FibonacciHeapNode> heapNodes = new HashMap<Node,FibonacciHeap<HeapObject>.FibonacciHeapNode>();
    FibonacciHeap<HeapObject> heap;
    Comparator<CostType> costComparator;

    public DijkstraPriorityQueueFibonacciImpl(
        final Comparator<CostType> costComparator )
    {
        super();
        this.costComparator = costComparator;
        heap = new FibonacciHeap<HeapObject>( new Comparator<HeapObject>()
        {
            public int compare( HeapObject o1, HeapObject o2 )
            {
                return costComparator.compare( o1.getCost(), o2.getCost() );
            }
        } );
    }

    public void decreaseValue( Node node, CostType newValue )
    {
        FibonacciHeap<HeapObject>.FibonacciHeapNode fNode = heapNodes
            .get( node );
        heap.decreaseKey( fNode, new HeapObject( node, newValue ) );
    }

    public Node extractMin()
    {
        HeapObject heapObject = heap.extractMin();
        if ( heapObject == null )
        {
            return null;
        }
        return heapObject.getNode();
    }

    public void insertValue( Node node, CostType value )
    {
        FibonacciHeap<HeapObject>.FibonacciHeapNode fNode = heap
            .insert( new HeapObject( node, value ) );
        heapNodes.put( node, fNode );
    }

    public boolean isEmpty()
    {
        return heap.isEmpty();
    }

    public Node peek()
    {
        FibonacciHeap<HeapObject>.FibonacciHeapNode fNode = heap.getMinimum();
        if ( fNode == null )
        {
            return null;
        }
        return fNode.getKey().getNode();
    }
}
