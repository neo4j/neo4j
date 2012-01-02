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
package org.neo4j.graphdb.index;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * A utility class for creating unique (with regard to a given index) entities.
 * 
 * Uses the {@link Index#putIfAbsent(PropertyContainer, String, Object) putIfAbsent() method} of the referenced index.
 * 
 * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
 * 
 * @param <T> the type of entity created by this {@link UniqueFactory}.
 */
public abstract class UniqueFactory<T extends PropertyContainer>
{
    /**
     * Implementation of {@link UniqueFactory} for {@link Node}.
     * 
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     */
    public static abstract class UniqueNodeFactory extends UniqueFactory<Node>
    {
        /**
         * Create a new {@link UniqueFactory} for nodes.
         * 
         * @param index the index to store entities uniquely in.
         */
        public UniqueNodeFactory( Index<Node> index )
        {
            super( index );
        }

        /**
         * Create a new {@link UniqueFactory} for nodes.
         * 
         * @param graphdb the graph database to get the index from.
         * @param index the name of the index to store entities uniquely in.
         */
        public UniqueNodeFactory( GraphDatabaseService graphdb, String index )
        {
            super( graphdb.index().forNodes( index ) );
        }

        /**
         * Implement this method to initialize the {@link Node} created for being stored in the index.
         * 
         * This method will be invoked exactly once per created unique node.
         * 
         * The created node might be discarded if another thread creates an node concurrently.
         * This method will however only be invoked in the transaction that succeeds in creating the node.
         * 
         * @param node the created node to initialize.
         * @param key the key under which this node is uniquely indexed.
         * @param value the value with which this node is uniquely indexed.
         */
        @Override
        protected abstract void initialize( Node node, String key, Object value );

        @Override
        final Node create( String key, Object value )
        {
            return graphDatabase().createNode();
        }

        @Override
        void delete( Node node )
        {
            node.delete();
        }
    }

    /**
     * Implementation of {@link UniqueFactory} for {@link Relationship}.
     * 
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     */
    public static abstract class UniqueRelationshipFactory extends UniqueFactory<Relationship>
    {
        /**
         * Create a new {@link UniqueFactory} for relationships.
         * 
         * @param index the index to store entities uniquely in.
         */
        public UniqueRelationshipFactory( Index<Relationship> index )
        {
            super( index );
        }

        /**
         * Create a new {@link UniqueFactory} for relationships.
         * 
         * @param graphdb the graph database to get the index from.
         * @param index the name of the index to store entities uniquely in.
         */
        public UniqueRelationshipFactory( GraphDatabaseService graphdb, String index )
        {
            super( graphdb.index().forRelationships( index ) );
        }

        /**
         * Implement this method to create the {@link Relationship} to index.
         * 
         * This method will be invoked exactly once per transaction that attempts to create an entry in the index.
         * The created relationship might be discarded if another thread creates a relationship with the same mapping concurrently.
         * 
         * @param key the key under which this relationship is uniquely indexed.
         * @param value the value with which this relationship is uniquely indexed.
         * @return the relationship to add to the index.
         */
        @Override
        protected abstract Relationship create( String key, Object value );

        @Override
        void initialize( Relationship relationship, String key, Object value )
        {
            // creation is done in create()
        }

        @Override
        void delete( Relationship relationship )
        {
            relationship.delete();
        }
    }

    /**
     * Get the indexed entity, creating it (exactly once) if no indexed entity exists.
     * @param key the key to find the entity under in the index.
     * @param value the value the key is mapped to for the entity in the index.
     * @return the unique entity in the index.
     */
    public final T getOrCreate( String key, Object value )
    {
        T result, created = null;
        try
        {
            do
            {
                result = index.get( key, value ).getSingle();
                if ( result != null ) return result;
                if ( created == null )
                {
                    created = create( key, value );
                    if ( created == null )
                        throw new AssertionError( "create() returned null" );
                }
                result = created;
                if ( index.putIfAbsent( result, key, value ) )
                {
                    initialize( result, key, value );
                    created = null;
                    return result;
                }
                else
                {
                    result = null; // try again
                }
            }
            while ( result == null );
            return result;
        }
        finally
        {
            if ( created != null ) delete( created );
        }
    }

    /**
     * Get the {@link GraphDatabaseService graph database} of the referenced index.
     * @return the {@link GraphDatabaseService graph database} of the referenced index.
     */
    protected final GraphDatabaseService graphDatabase()
    {
        return index.getGraphDatabase();
    }

    /**
     * Get the referenced index.
     * @return the referenced index.
     */
    protected final Index<T> index()
    {
        return index;
    }

    private final Index<T> index;

    private UniqueFactory( Index<T> index )
    {
        this.index = index;
    }

    abstract T create( String key, Object value );

    abstract void initialize( T created, String key, Object value );

    abstract void delete( T result );
}
