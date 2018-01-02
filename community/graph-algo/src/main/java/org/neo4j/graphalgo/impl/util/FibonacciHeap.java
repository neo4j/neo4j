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
package org.neo4j.graphalgo.impl.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * At least a partial implementation of a Fibonacci heap (a priority heap).
 * Almost all code is based on the chapter about Fibonacci heaps in the book
 * "Introduction to Algorithms" by Cormen, Leiserson, Rivest and Stein (second
 * edition, 2001). Amortized times for almost all operations are O(1).
 * extractMin() runs in amortized time O(log n), which then a delete() based
 * upon it also would. This Fibonacci heap can store any datatype, given by the
 * KeyType parameter, all it needs is a comparator for that type. To achieve the
 * stated running times, it is needed that this comparator can do comparisons in
 * constant time (usually the case).
 * @author Patrik Larsson
 * @param <KeyType>
 *            The datatype to be stored in this heap.
 */
public class FibonacciHeap<KeyType>
{
    /**
     * One entry in the fibonacci heap is stored as an instance of this class.
     * References to such entries are required for some operations, like
     * decreaseKey().
     */
    public class FibonacciHeapNode
    {
        FibonacciHeapNode left, right, parent, child;
        boolean marked = false;
        KeyType key;
        int degree = 0;

        public FibonacciHeapNode( KeyType key )
        {
            super();
            this.key = key;
            left = this;
            right = this;
        }

        /**
         * @return the key
         */
        public KeyType getKey()
        {
            return key;
        }
    }

    Comparator<KeyType> keyComparator;
    FibonacciHeapNode minimum;
    int nrNodes = 0;

    public FibonacciHeap( Comparator<KeyType> keyComparator )
    {
        super();
        this.keyComparator = keyComparator;
    }

    /**
     * @return True if the heap is empty.
     */
    public boolean isEmpty()
    {
        return minimum == null;
    }

    /**
     * @return The number of entries in this heap.
     */
    public int size()
    {
        return nrNodes;
    }

    /**
     * @return The entry with the highest priority or null if the heap is empty.
     */
    public FibonacciHeapNode getMinimum()
    {
        return minimum;
    }

    /**
     * Internal helper function for moving nodes into the root list
     */
    protected void insertInRootList( FibonacciHeapNode fNode )
    {
        fNode.parent = null;
        fNode.marked = false;
        if ( minimum == null )
        {
            minimum = fNode;
            minimum.right = minimum;
            minimum.left = minimum;
        }
        else
        {
            // insert in root list
            fNode.left = minimum.left;
            fNode.right = minimum;
            fNode.left.right = fNode;
            fNode.right.left = fNode;
            if ( keyComparator.compare( fNode.key, minimum.key ) < 0 )
            {
                minimum = fNode;
            }
        }
    }

    /**
     * Inserts a new value into the heap.
     * @param key
     *            the value to be inserted.
     * @return The entry made into the heap.
     */
    public FibonacciHeapNode insert( KeyType key )
    {
        FibonacciHeapNode node = new FibonacciHeapNode( key );
        insertInRootList( node );
        ++nrNodes;
        return node;
    }

    /**
     * Creates the union of two heaps by absorbing the other into this one.
     * Note: Destroys other
     */
    public void union( FibonacciHeap<KeyType> other )
    {
        nrNodes += other.nrNodes;
        if ( other.minimum == null )
        {
            return;
        }
        if ( minimum == null )
        {
            minimum = other.minimum;
            return;
        }
        // swap left nodes
        FibonacciHeapNode otherLeft = other.minimum.left;
        other.minimum.left = minimum.left;
        minimum.left = otherLeft;
        // update their right pointers
        minimum.left.right = minimum;
        other.minimum.left.right = other.minimum;
        // get min
        if ( keyComparator.compare( other.minimum.key, minimum.key ) < 0 )
        {
            minimum = other.minimum;
        }
    }

    /**
     * This removes and returns the entry with the highest priority.
     * @return The value with the highest priority.
     */
    public KeyType extractMin()
    {
        if ( minimum == null )
        {
            return null;
        }
        FibonacciHeapNode minNode = minimum;
        // move all children to root list
        if ( minNode.child != null )
        {
            FibonacciHeapNode child = minNode.child;
            while ( minNode.equals( child.parent ) )
            {
                FibonacciHeapNode nextChild = child.right;
                insertInRootList( child );
                child = nextChild;
            }
        }
        // remove minNode from root list
        minNode.left.right = minNode.right;
        minNode.right.left = minNode.left;
        // update minimum
        if ( minNode.right.equals( minNode ) )
        {
            minimum = null;
        }
        else
        {
            minimum = minimum.right;
            consolidate();
        }
        --nrNodes;
        return minNode.key;
    }

    /**
     * Internal helper function.
     */
    protected void consolidate()
    {
        // TODO: lower the size of this (log(n))
        int arraySize = nrNodes + 1;
        // arraySize = 2;
        // for ( int a = nrNodes + 1; a < 0; a /= 2 )
        // {
        // arraySize++;
        // }
        // arraySize = (int) Math.log( (double) nrNodes )+1;
        // FibonacciHeapNode[] A = (FibonacciHeapNode[]) new Object[arraySize];
        // FibonacciHeapNode[] A = new FibonacciHeapNode[arraySize];
        ArrayList<FibonacciHeapNode> A = new ArrayList<FibonacciHeapNode>(
            arraySize );
        for ( int i = 0; i < arraySize; ++i )
        {
            A.add( null );
        }
        List<FibonacciHeapNode> rootNodes = new LinkedList<FibonacciHeapNode>();
        rootNodes.add( minimum );
        for ( FibonacciHeapNode n = minimum.right; !n.equals( minimum ); n = n.right )
        {
            rootNodes.add( n );
        }
        for ( FibonacciHeapNode node : rootNodes )
        {
            // no longer a root node?
            if ( node.parent != null )
            {
                continue;
            }
            int d = node.degree;
            while ( A.get( d ) != null )
            {
                FibonacciHeapNode y = A.get( d );
                // swap?
                if ( keyComparator.compare( node.key, y.key ) > 0 )
                {
                    FibonacciHeapNode tmp = node;
                    node = y;
                    y = tmp;
                }
                link( y, node );
                A.set( d, null );
                ++d;
            }
            A.set( d, node );
        }
        // throw away the root list
        minimum = null;
        // and rebuild it from A
        for ( FibonacciHeapNode node : A )
        {
            if ( node != null )
            {
                insertInRootList( node );
            }
        }
    }

    /**
     * Internal helper function. Makes root node y a child of root node x.
     */
    protected void link( FibonacciHeapNode y, FibonacciHeapNode x )
    {
        // remove y from root list
        y.left.right = y.right;
        y.right.left = y.left;
        // make y a child of x
        if ( x.child == null ) // no previous children?
        {
            y.right = y;
            y.left = y;
        }
        else
        {
            y.left = x.child.left;
            y.right = x.child;
            y.right.left = y;
            y.left.right = y;
        }
        x.child = y;
        y.parent = x;
        // adjust degree and mark
        x.degree++;
        y.marked = false;
    }

    /**
     * Raises the priority for an entry.
     * @param node
     *            The entry to recieve a higher priority.
     * @param newKey
     *            The new value.
     */
    public void decreaseKey( FibonacciHeapNode node, KeyType newKey )
    {
        if ( keyComparator.compare( newKey, node.key ) > 0 )
        {
            throw new RuntimeException( "Trying to decrease to a greater key" );
        }
        node.key = newKey;
        FibonacciHeapNode parent = node.parent;
        if ( parent != null
            && keyComparator.compare( node.key, parent.key ) < 0 )
        {
            cut( node, parent );
            cascadingCut( parent );
        }
        if ( keyComparator.compare( node.key, minimum.key ) < 0 )
        {
            minimum = node;
        }
    }

    /**
     * Internal helper function. This removes y's child x and moves x to the
     * root list.
     */
    protected void cut( FibonacciHeapNode x, FibonacciHeapNode y )
    {
        // remove x from child list of y
        x.left.right = x.right;
        x.right.left = x.left;
        if ( x.right.equals( x ) )
        {
            y.child = null;
        }
        else
        {
            y.child = x.right;
        }
        y.degree--;
        // add x to root list
        insertInRootList( x );
    }

    /**
     * Internal helper function.
     */
    protected void cascadingCut( FibonacciHeapNode y )
    {
        FibonacciHeapNode parent = y.parent;
        if ( parent != null )
        {
            if ( !parent.marked )
            {
                parent.marked = true;
            }
            else
            {
                cut( y, parent );
                cascadingCut( parent );
            }
        }
    }
}
