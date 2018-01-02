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
package org.neo4j.test.mocking;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Link
{
    public static Link link( Relationship relationship, Node node )
    {
        if ( relationship.getStartNode().getId() == node.getId() )
        {
            return new Link( node, relationship );
        }
        if ( relationship.getEndNode().getId() == node.getId() )
        {
            return new Link( relationship, node );
        }
        throw illegalArgument( "%s is neither the start node nor the end node of %s", node, relationship );
    }

    final Relationship relationship;
    private final Node node;
    private final boolean isStartNode;

    private Link( Node node, Relationship relationship )
    {
        this.relationship = relationship;
        this.node = node;
        this.isStartNode = true;
    }

    private Link( Relationship relationship, Node node )
    {
        this.relationship = relationship;
        this.node = node;
        this.isStartNode = false;
    }

    public Node checkNode( Node node )
    {
        if ( isStartNode )
        {
            if ( node.getId() != relationship.getEndNode().getId() )
            {
                throw illegalArgument( "%s is not the end node of %s", node, relationship );
            }
        }
        else
        {
            if ( node.getId() != relationship.getStartNode().getId() )
            {
                throw illegalArgument( "%s is not the start node of %s", node, relationship );
            }
        }
        return this.node;
    }

    private static IllegalArgumentException illegalArgument( String message, Object... parameters )
    {
        return new IllegalArgumentException( String.format( message, parameters ) );
    }
}
