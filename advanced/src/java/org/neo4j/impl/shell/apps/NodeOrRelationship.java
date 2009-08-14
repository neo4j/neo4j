/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.impl.shell.apps;

import java.util.ArrayList;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

/**
 * This is a wrapper class for a Node or a Relationship, since some apps deals
 * with a nodes or a relationships properties and shouldn't need to duplicate
 * that code. Please make this interface follow the Node/Relationship
 * interfaces.
 */
public abstract class NodeOrRelationship
{
    public static final String TYPE_NODE = "n";
    public static final String TYPE_RELATIONSHIP = "r";
    
    public static NodeOrRelationship wrap( Node node )
    {
        return new WrapNode( node );
    }

    public static NodeOrRelationship wrap( Relationship rel )
    {
        return new WrapRelationship( rel );
    }

    private Object nodeOrRelationship;

    private NodeOrRelationship( Object nodeOrRelationship )
    {
        this.nodeOrRelationship = nodeOrRelationship;
    }
    
    public boolean isNode()
    {
        return nodeOrRelationship instanceof Node;
    }

    public Node asNode()
    {
        return (Node) nodeOrRelationship;
    }

    public boolean isRelationship()
    {
        return nodeOrRelationship instanceof Relationship;
    }

    public Relationship asRelationship()
    {
        return (Relationship) nodeOrRelationship;
    }
    
    public TypedId getTypedId()
    {
        return new TypedId( getType(), getId() );
    }
    
    abstract String getType();
    
    public abstract long getId();

    public abstract boolean hasProperty( String key );

    public abstract Object getProperty( String key );

    public abstract Object getProperty( String key, Object defaultValue );

    public abstract void setProperty( String key, Object value );

    public abstract Object removeProperty( String key );

    public abstract Iterable<String> getPropertyKeys();

    public abstract Iterable<Relationship> getRelationships( Direction direction );

    static class WrapNode extends NodeOrRelationship
    {
        private WrapNode( Node node )
        {
            super( node );
        }

        private Node object()
        {
            return asNode();
        }
        
        @Override
        public String getType()
        {
            return TYPE_NODE;
        }

        @Override
        public long getId()
        {
            return object().getId();
        }

        @Override
        public boolean hasProperty( String key )
        {
            return object().hasProperty( key );
        }

        @Override
        public Object getProperty( String key )
        {
            return object().getProperty( key );
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            return object().getProperty( key, defaultValue );
        }

        @Override
        public void setProperty( String key, Object value )
        {
            object().setProperty( key, value );
        }

        @Override
        public Object removeProperty( String key )
        {
            return object().removeProperty( key );
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            return object().getPropertyKeys();
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction direction )
        {
            return object().getRelationships( direction );
        }
    }

    static class WrapRelationship extends NodeOrRelationship
    {
        private WrapRelationship( Relationship rel )
        {
            super( rel );
        }

        private Relationship object()
        {
            return asRelationship();
        }
        
        @Override
        public String getType()
        {
            return TYPE_RELATIONSHIP;
        }

        @Override
        public long getId()
        {
            return object().getId();
        }

        @Override
        public boolean hasProperty( String key )
        {
            return object().hasProperty( key );
        }

        @Override
        public Object getProperty( String key )
        {
            return object().getProperty( key );
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            return object().getProperty( key, defaultValue );
        }

        @Override
        public void setProperty( String key, Object value )
        {
            object().setProperty( key, value );
        }

        @Override
        public Object removeProperty( String key )
        {
            return object().removeProperty( key );
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            return object().getPropertyKeys();
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction direction )
        {
            return new ArrayList<Relationship>();
        }
    }
    
    public static class TypedId
    {
        private final String type;
        private final long id;
        private final boolean isNode;
        
        public TypedId( String typedId )
        {
            this( typedId.substring( 0, 1 ),
                Long.parseLong( typedId.substring( 1 ) ) );
        }
        
        public TypedId( String type, long id )
        {
            this.type = type;
            this.id = id;
            this.isNode = type.equals( TYPE_NODE );
        }
        
        public String getType()
        {
            return this.type;
        }
        
        public long getId()
        {
            return this.id;
        }
        
        public boolean isNode()
        {
            return this.isNode;
        }
        
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
}