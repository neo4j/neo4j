/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.traversal;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

public final class FinalTraversalBranch implements TraversalBranch
{
    private final Node head;
    private final Relationship[] path;

    public FinalTraversalBranch( Node head, Relationship... path )
    {
        this.head = head;
        this.path = path;
    }

    public int length()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Returns <code>null</code> since {@link FinalTraversalBranch} does not
     * expand.
     */
    @Override
    public TraversalBranch next( PathExpander expander, TraversalContext metadata )
    {
        return null;
    }
    
    @Override
    public void prune()
    {
    }

    public Node endNode()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Node startNode()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public TraversalBranch parent()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Relationship lastRelationship()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public int expanded()
    {
        return 0;
    }
    
    @Override
    public boolean includes()
    {
        return true;
    }
    
    @Override
    public boolean continues()
    {
        return false;
    }

    @Override
    public void evaluation( Evaluation eval )
    {
    }
    
    public void initialize( PathExpander expander, TraversalContext metadata )
    {
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<Node> nodes()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Iterable<Node> reverseNodes()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Object state()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
