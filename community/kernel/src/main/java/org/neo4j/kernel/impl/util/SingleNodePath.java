/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

public class SingleNodePath implements Path
{
    private final Node node;

    public SingleNodePath( Node node )
    {
        this.node = node;
    }
    
    @Override
    public Node startNode()
    {
        return node;
    }

    @Override
    public Node endNode()
    {
        return node;
    }

    @Override
    public Relationship lastRelationship()
    {
        return null;
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return Collections.emptyList();
    }
    
    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        return relationships();
    }

    @Override
    public Iterable<Node> nodes()
    {
        return Arrays.asList( node );
    }
    
    @Override
    public Iterable<Node> reverseNodes()
    {
        return nodes();
    }

    @Override
    public int length()
    {
        return 0;
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        return Arrays.<PropertyContainer>asList( node ).iterator();
    }
}
