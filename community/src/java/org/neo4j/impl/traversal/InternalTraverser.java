/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.traversal;

import java.util.Collection;

import org.neo4j.api.core.Node;

/**
 * A utility class that represents an arbitrary traversal over a nodespace. A
 * Traverser is used as a tool for a NEO client programmer to easily traverse
 * the node space according a complex set of rules.
 * <P>
 * A Traverser is created by invoking any of the factory methods in
 * {@link InternalTraverserFactory}. Subsequent retrieval of the nodes from a Traverser
 * is simple: it can either be treated as an {@link java.util.Iterator} using
 * the standard <CODE>if ( i.hasNext() ) { i.next(); }</CODE> idiom or as a
 * Traverser with the type-safe {@link #nextNode} or the array-based
 * {@link #getAllNodes} operations.
 * <P>
 * A Traverser is generally not thread safe. If a client wishes to use it in a
 * concurrent environment, they are strongly adviced to synchronize externally.
 */
public interface InternalTraverser extends org.neo4j.api.core.Traverser
{
    /**
     * Returns the next node in the traversal.
     * @throws java.util.NoSuchElementException
     *             if the traversal has no more nodes
     * @return the next node in the traversal
     * @see #nextNode
     */
    public Object next();

    /**
     * Returns <CODE>true</CODE> if the traversal has more nodes. In other
     * words, returns <CODE>true</CODE> if {@link #next} or {@link #nextNode}
     * would return a node rather than throwing an exception.
     * @return <CODE>true</CODE> if the traversal has more nodes
     */
    public boolean hasNext();

    /**
     * Unsupported, throws an {@link java.lang.UnsupportedOperationException}.
     * @throws java.lang.UnsupportedOperationException
     *             always when this operation is invoked
     */
    public void remove();

    /**
     * Returns the next node in the traversal. This operation is identical to
     * {@link #next} except that it's type safe.
     * @return the next node in the traversal
     * @throws java.util.NoSuchElementException
     *             if the traversal has no more nodes
     */
    public Node nextNode();

    /**
     * Returns all remaining nodes in the traversal as an array, <B>PLEASE NOTE</B>
     * that this operation can potentially be VERY CPU consuming and leave
     * little room for performance-boosters such as background streaming of
     * content from persistence and cache spooling. Only use this method if you
     * know that the traverser is well-behaving and won't run wild and try to
     * return the entire node space.
     * @return an array of all the nodes in this traversal
     */
    public Collection<Node> getAllNodes();

    /**
     * Returns a Traverser for all remaining nodes sorted as specified by 
     * <CODE>NodeSortInfo</CODE>.
     * <B>PLEASE NOTE</B> that this operation can potentially be VERY CPU
     * consuming and leave little room for performance-boosters such as
     * background streaming of content from persistence and cache spooling. Only
     * use this method if you know that the traverser is well-behaving and won't
     * run wild and try to return the entire node space.
     * 
     * @param nsi
     *            encapsulates how the nodes should be sorted
     * @return a sorted traverser for all remaining nodes
     */
    public InternalTraverser sort( NodeSortInfo<Node> nsi );
}