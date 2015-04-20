/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.ha.lock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LockableRelationship implements Relationship
{
    private final long id;

    public LockableRelationship( long id )
    {
        this.id = id;
    }

    public void delete()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node getEndNode()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public long getId()
    {
        return this.id;
    }

    public GraphDatabaseService getGraphDatabase()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node[] getNodes()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node getOtherNode( Node node )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Object getProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Iterable<String> getPropertyKeys()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Node getStartNode()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public RelationshipType getType()
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public boolean isType( RelationshipType type )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public boolean hasProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public Object removeProperty( String key )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public void setProperty( String key, Object value )
    {
        throw new UnsupportedOperationException( "Lockable rel" );
    }

    public boolean equals( Object o )
    {
        if ( !(o instanceof Relationship) )
        {
            return false;
        }
        return this.getId() == ((Relationship) o).getId();
    }

    public int hashCode()
    {
        return (int) (( id >>> 32 ) ^ id );
    }

    public String toString()
    {
        return "Lockable relationship #" + this.getId();
    }
}
