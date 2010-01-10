/*
 * Copyright (c) 2008-2009 "Neo Technology,"
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
package org.neo4j.remote;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

final class RemoteRelationship extends RemotePropertyContainer implements
    Relationship
{
    private final RelationshipType type;
    private final RemoteNode startNode;
    private final RemoteNode endNode;

    RemoteRelationship( RemoteGraphDbEngine txService, long id,
        RelationshipType type, RemoteNode start, RemoteNode end )
    {
        super( txService, id );
        this.type = type;
        this.startNode = start;
        this.endNode = end;
    }

    @Override
    public int hashCode()
    {
        return ( int ) id;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof RemoteRelationship )
        {
            RemoteRelationship relationship = ( RemoteRelationship ) obj;
            return relationship.id == id && relationship.engine.equals( engine );
        }
        else
        {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return "Relationship[" + id + "]";
    }

    public long getId()
    {
        return id;
    }

    public void delete()
    {
        engine.current().deleteRelationship( this );
    }

    public Node getStartNode()
    {
        return startNode;
    }

    public Node getEndNode()
    {
        return endNode;
    }

    public Node[] getNodes()
    {
        return new Node[] { startNode, endNode };
    }

    public Node getOtherNode( Node node )
    {
        if ( node.equals( startNode ) )
        {
            return endNode;
        }
        else if ( node.equals( endNode ) )
        {
            return startNode;
        }
        else
        {
            throw new RuntimeException( "Node[" + node.getId()
                + "] not connected to Relationship[" + getId() + "]" );
        }
    }

    public RelationshipType getType()
    {
        return type;
    }

    public boolean isType( @SuppressWarnings( "hiding" ) RelationshipType type )
    {
        return this.type.name().equals( type.name() );
    }

    @Override
    Object getContainerProperty( String key )
    {
        return engine.current().getProperty( this, key );
    }

    public Iterable<String> getPropertyKeys()
    {
        return engine.current().getPropertyKeys( this );
    }

    public boolean hasProperty( String key )
    {
        return engine.current().hasProperty( this, key );
    }

    public Object removeProperty( String key )
    {
        return engine.current().removeProperty( this, key );
    }

    public void setProperty( String key, Object value )
    {
        engine.current().setProperty( this, key, value );
    }
}
