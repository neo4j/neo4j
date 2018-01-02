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
package org.neo4j.shell.kernel.apps;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * An id with a type, {@link Node} or {@link Relationship}) associated with it.
 */
class TypedId
{
    private final String type;
    private final long id;
    private final boolean isNode;
    
    /**
     * @param typedId the serialized string.
     */
    public TypedId( String typedId )
    {
        this( typedId.substring( 0, 1 ),
            Long.parseLong( typedId.substring( 1 ) ) );
    }
    
    /**
     * @param type the type
     * @param id the object's id.
     */
    public TypedId( String type, long id )
    {
        this.type = type;
        this.id = id;
        this.isNode = type.equals( NodeOrRelationship.TYPE_NODE );
    }
    
    /**
     * @return the type.
     */
    public String getType()
    {
        return this.type;
    }
    
    /**
     * @return the object's id.
     */
    public long getId()
    {
        return this.id;
    }
    
    /**
     * @return whether or not the type is a {@link Node}.
     */
    public boolean isNode()
    {
        return this.isNode;
    }
    
    /**
     * @return whether or not the type is a {@link Relationship}.
     */
    public boolean isRelationship()
    {
        return !this.isNode;
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( !( o instanceof TypedId ) )
        {
            return false;
        }
        TypedId other = ( TypedId ) o;
        return this.type.equals( other.type ) &&
            this.id == other.id;
    }
    
    @Override
    public int hashCode()
    {
        int code = 7;
        code = 31 * code + new Long( this.id ).hashCode();
        code = 31 * code + this.type.hashCode();
        return code;
    }
    
    @Override
    public String toString()
    {
        return this.type + this.id;
    }
}
