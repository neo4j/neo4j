/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.TraversalBranch;

public final class FinalTraversalBranch implements TraversalBranch
{
    private final Node head;
    private final Relationship[] path;

    public FinalTraversalBranch( Node head, Relationship... path )
    {
        this.head = head;
        this.path = path;
    }

    public int depth()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Returns <code>null</code> since {@link FinalTraversalBranch} does not
     * expand.
     */
    public TraversalBranch next()
    {
        return null;
    }

    public Node node()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public TraversalBranch parent()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Path position()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Relationship relationship()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public int expanded()
    {
        return 0;
    }

    public Evaluation evaluation()
    {
        return null;
    }
    
    public void initialize()
    {
    }
}
