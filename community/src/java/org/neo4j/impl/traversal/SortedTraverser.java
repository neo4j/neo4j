/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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

import org.neo4j.api.core.Node;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;

/**
 * This class is used for traversers that has been sorted. A normal traverser
 * will return a <CODE>SortedTraverser</CODE> when the <CODE>sort</CODE>
 * method is invoked. Since a sorting operation requires one to go throu all
 * elements that should be sorted the <CODE>SortedTraverser</CODE> just
 * encapsulates an iterator to the sorted list of nodes.
 */
class SortedTraverser implements Traverser
{
    private Iterator<Node> nodesIterator = null;

    /**
     * Creates a sorted traverser from the sorted list of <CODE>nodes</CODE>
     * 
     * @param nodes
     *            the list of sorted nodes
     */
    SortedTraverser( List<Node> nodes )
    {
        this.nodesIterator = nodes.iterator();
    }

    // javadoc: see java.util.Iterator
    public Object next()
    {
        return nextNode();
    }

    // javadoc: see java.util.Iterator
    public boolean hasNext()
    {
        return nodesIterator.hasNext();
    }

    // javadoc: see java.util.Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // javadoc: see Traverser
    public Node nextNode()
    {
        return nodesIterator.next();
    }

    // javadoc: see Traverser
    public Collection<Node> getAllNodes()
    {
        // Temp storage
        List<Node> tempList = new ArrayList<Node>();

        // Traverse until the end, my beautiful friend
        while ( this.hasNext() )
        {
            tempList.add( this.nextNode() );
        }

        // Return nodes
        return tempList;
    }

    // javadoc: see Traverser
    public Traverser sort( NodeSortInfo<Node> nsi )
    {
        ArrayList<Node> tempList = new ArrayList<Node>();

        // Traverse and get all remaining nodes
        while ( this.hasNext() )
        {
            tempList.add( this.nextNode() );
        }

        Collections.sort( tempList, nsi );
        return new SortedTraverser( tempList );
    }

    public TraversalPosition currentPosition()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Iterator<Node> iterator()
    {
        // TODO Auto-generated method stub
        return null;
    }
}