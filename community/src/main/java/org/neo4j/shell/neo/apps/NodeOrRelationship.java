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
package org.neo4j.shell.neo.apps;

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
abstract class NodeOrRelationship
{
    /**
     * The type which identifies a {@link TypedId} that it is a {@link Node}.
     */
    public static final String TYPE_NODE = "n";
    /**
     * The type which identifies a {@link TypedId} that it is a{@link Relationship}.
     */
    public static final String TYPE_RELATIONSHIP = "r";
    
    /**
     * @param node the {@link Node} to wrap.
     * @return a {@link Node} wrapped in a {@link NodeOrRelationship}.
     */
    public static NodeOrRelationship wrap( Node node )
    {
        return new WrapNode( node );
    }

    /**
     * @param rel the {@link Relationship} to wrap.
     * @return a {@link Relationship} wrapped in a {@link NodeOrRelationship}.
     */
    public static NodeOrRelationship wrap( Relationship rel )
    {
        return new WrapRelationship( rel );
    }

    private Object nodeOrRelationship;

    private NodeOrRelationship( Object nodeOrRelationship )
    {
        this.nodeOrRelationship = nodeOrRelationship;
    }
    
    /**
     * @return whether or not the underlying object is a {@link Node}. 
     */
    public boolean isNode()
    {
        return nodeOrRelationship instanceof Node;
    }

    /**
     * @return the underlying {@link Node} if {@link #isNode()} should return
     * {@code true}, otherwise a {@link ClassCastException} will be thrown.
     */
    public Node asNode()
    {
        return (Node) nodeOrRelationship;
    }

    /**
     * @return whether or not the underlying object is a {@link Relationship}. 
     */
    public boolean isRelationship()
    {
        return nodeOrRelationship instanceof Relationship;
    }

    /**
     * @return the underlying {@link Relationship} if {@link #isRelationship()}
     * should return {@code true}, otherwise a {@link ClassCastException}
     * will be thrown.
     */
    public Relationship asRelationship()
    {
        return (Relationship) nodeOrRelationship;
    }
    
    /**
     * @return the {@link TypedId} for this 
     */
    public TypedId getTypedId()
    {
        return new TypedId( getType(), getId() );
    }
    
    abstract String getType();
    
    /**
     * @return the underlying objects id.
     */
    public abstract long getId();

    /**
     * @param key the property key
     * @return whether or not the underlying object has property {@code key}.
     */
    public abstract boolean hasProperty( String key );

    /**
     * @param key the property key
     * @return the result from the underlying object
     */
    public abstract Object getProperty( String key );

    /**
     * @param key the property key
     * @param defaultValue the default value
     * @return the result from the underlying object
     */
    public abstract Object getProperty( String key, Object defaultValue );

    /**
     * @param key the property key
     * @param value the property value
     */
    public abstract void setProperty( String key, Object value );

    /**
     * @param key the property key
     * @return the old property value or {@code null} if there was none.
     */
    public abstract Object removeProperty( String key );

    /**
     * @return the property keys for the underlying object.
     */
    public abstract Iterable<String> getPropertyKeys();

    /**
     * @param direction the {@link Direction} of the relationships.
     * @return the {@link Relationship}s for the underlying object.
     */
    public abstract Iterable<Relationship> getRelationships(
        Direction direction );

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
}