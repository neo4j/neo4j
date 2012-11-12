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
package org.neo4j.tooling.wrap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public abstract class WrappedRelationship<G extends WrappedGraphDatabase> extends WrappedEntity<G, Relationship>
        implements Relationship
{
    protected WrappedRelationship( G graphdb )
    {
        super( graphdb );
    }

    @Override
    public long getId()
    {
        return actual().getId();
    }

    @Override
    public void delete()
    {
        actual().delete();
    }

    @Override
    public Node getStartNode()
    {
        return graphdb.node( actual().getStartNode(), false );
    }

    @Override
    public Node getEndNode()
    {
        return graphdb.node( actual().getEndNode(), false );
    }

    @Override
    public Node getOtherNode( Node node )
    {
        return graphdb.node( actual().getOtherNode( unwrap( node ) ), false );
    }

    @Override
    public Node[] getNodes()
    {
        Node[] nodes = actual().getNodes(), wrapped = new Node[nodes.length];
        for ( int i = 0; i < wrapped.length; i++ )
            wrapped[i] = graphdb.node( nodes[i], false );
        return wrapped;
    }

    @Override
    public RelationshipType getType()
    {
        return actual().getType();
    }

    @Override
    public boolean isType( RelationshipType type )
    {
        return actual().isType( type );
    }
}
