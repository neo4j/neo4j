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
package org.neo4j.impl.core;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;

class RelationshipProxy implements Relationship
{
    private final int relId;
    private final NodeManager nm;

    RelationshipProxy( int relId, NodeManager nodeManager )
    {
        this.relId = relId;
        this.nm = nodeManager;
    }

    public long getId()
    {
        return relId;
    }

    public void delete()
    {
        nm.getRelForProxy( relId ).delete();
    }

    public Node[] getNodes()
    {
        return nm.getRelForProxy( relId ).getNodes();
    }

    public Node getOtherNode( Node node )
    {
        return nm.getRelForProxy( relId ).getOtherNode( node );
    }

    public Node getStartNode()
    {
        return nm.getRelForProxy( relId ).getStartNode();
    }

    public Node getEndNode()
    {
        return nm.getRelForProxy( relId ).getEndNode();
    }

    public RelationshipType getType()
    {
        return nm.getRelForProxy( relId ).getType();
    }

    public Iterable<String> getPropertyKeys()
    {
        return nm.getRelForProxy( relId ).getPropertyKeys();
    }

    public Iterable<Object> getPropertyValues()
    {
        return nm.getRelForProxy( relId ).getPropertyValues();
    }

    public Object getProperty( String key )
    {
        return nm.getRelForProxy( relId ).getProperty( key );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return nm.getRelForProxy( relId ).getProperty( key, defaultValue );
    }

    public boolean hasProperty( String key )
    {
        return nm.getRelForProxy( relId ).hasProperty( key );
    }

    public void setProperty( String key, Object property )
    {
        nm.getRelForProxy( relId ).setProperty( key, property );
    }

    public Object removeProperty( String key )
    {
        return nm.getRelForProxy( relId ).removeProperty( key );
    }

    public boolean isType( RelationshipType type )
    {
        return nm.getRelForProxy( relId ).isType( type );
    }

    public int compareTo( Object rel )
    {
        Relationship r = (Relationship) rel;
        int ourId = (int) this.getId(), theirId = (int) r.getId();

        if ( ourId < theirId )
        {
            return -1;
        }
        else if ( ourId > theirId )
        {
            return 1;
        }
        else
        {
            return 0;
        }
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
        return relId;
    }

    public String toString()
    {
        return "Relationship[" + this.getId() + "]";
    }
}