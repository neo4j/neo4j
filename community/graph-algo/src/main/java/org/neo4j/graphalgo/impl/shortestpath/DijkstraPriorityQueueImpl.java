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
import java.util.PriorityQueue;

import org.neo4j.graphdb.Node;

/**
 * Implementation of {@link DijkstraPriorityQueue} with just a normal java
 * priority queue.
 * @param <CostType>
 *            The datatype the path weigths are represented by.
 */
public class DijkstraPriorityQueueImpl<CostType> implements
    DijkstraPriorityQueue<CostType>
{
    /**
     * Data structure used for the internal priority queue
     */
    protected class pathObject
    {
        private Node node;
        private CostType cost;

        public pathObject( Node node, CostType cost )
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
            final pathObject other = (pathObject) obj;
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
            int result = node != null ? node.hashCode() : 0;
            result = 31 * result + (cost != null ? cost.hashCode() : 0);
            return result;
        }
    }

    Comparator<CostType> costComparator;
    PriorityQueue<pathObject> queue;

    public DijkstraPriorityQueueImpl( final Comparator<CostType> costComparator )
    {
        super();
        this.costComparator = costComparator;
        queue = new PriorityQueue<pathObject>( 11, new Comparator<pathObject>()
        {
            public int compare( pathObject o1, pathObject o2 )
            {
                return costComparator.compare( o1.getCost(), o2.getCost() );
            }
        } );
    }

    public void insertValue( Node node, CostType value )
    {
        queue.add( new pathObject( node, value ) );
    }

    public void decreaseValue( Node node, CostType newValue )
    {
        pathObject po = new pathObject( node, newValue );
        // Shake the queue
        // remove() will remove the old pathObject
        // BUT IT TAKES A LOT OF TIME FOR SOME REASON
        // queue.remove( po );
        queue.add( po );
    }

    /**
     * Retrieve and remove
     */
    public Node extractMin()
    {
        pathObject po = queue.poll();
        if ( po == null )
        {
            return null;
        }
        return po.getNode();
    }

    /**
     * Retrieve without removing
     */
    public Node peek()
    {
        pathObject po = queue.peek();
        if ( po == null )
        {
            return null;
        }
        return po.getNode();
    }

    public boolean isEmpty()
    {
        return queue.isEmpty();
    }
}
