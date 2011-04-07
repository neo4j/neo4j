/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

class RelationshipProxy implements Relationship
{
    private final long relId;
    private final NodeManager nm;

    RelationshipProxy( long relId, NodeManager nodeManager )
    {
        this.relId = relId;
        this.nm = nodeManager;
    }

    public long getId()
    {
        return relId;
    }
    
    public GraphDatabaseService getGraphDatabase()
    {
        return nm.getGraphDbService();
    }

    public void delete()
    {
        nm.getRelForProxy( relId ).delete( nm );
    }

    public Node[] getNodes()
    {
        return nm.getRelForProxy( relId ).getNodes( nm );
    }

    public Node getOtherNode( Node node )
    {
        return nm.getRelForProxy( relId ).getOtherNode( nm, node );
    }

    public Node getStartNode()
    {
        return nm.getRelForProxy( relId ).getStartNode( nm );
    }

    public Node getEndNode()
    {
        return nm.getRelForProxy( relId ).getEndNode( nm );
    }

    public RelationshipType getType()
    {
        return nm.getRelForProxy( relId ).getType();
    }

    public Iterable<String> getPropertyKeys()
    {
        return nm.getRelForProxy( relId ).getPropertyKeys( nm );
    }

    public Iterable<Object> getPropertyValues()
    {
        return nm.getRelForProxy( relId ).getPropertyValues( nm );
    }

    public Object getProperty( String key )
    {
        return nm.getRelForProxy( relId ).getProperty( nm, key );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return nm.getRelForProxy( relId ).getProperty( nm, key, defaultValue );
    }

    public boolean hasProperty( String key )
    {
        return nm.getRelForProxy( relId ).hasProperty( nm, key );
    }

    public void setProperty( String key, Object property )
    {
        nm.getRelForProxy( relId ).setProperty( nm, key, property );
    }

    public Object removeProperty( String key )
    {
        return nm.getRelForProxy( relId ).removeProperty( nm, key );
    }

    public boolean isType( RelationshipType type )
    {
        return nm.getRelForProxy( relId ).isType( type );
    }

    public int compareTo( Object rel )
    {
        Relationship r = (Relationship) rel;
        long ourId = this.getId(), theirId = r.getId();

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

    @Override
    public boolean equals( Object o )
    {
        if ( !(o instanceof Relationship) )
        {
            return false;
        }
        return this.getId() == ((Relationship) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) (( relId >>> 32 ) ^ relId );
    }

    @Override
    public String toString()
    {
        return "Relationship[" + this.getId() + "]";
    }
}