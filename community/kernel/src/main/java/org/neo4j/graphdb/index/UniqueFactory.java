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

import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

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
         * Default implementation of {@link UniqueFactory#create(Map)}, creates a plain node. Override to
         * retrieve the node to add to the index by some other means than by creating it. For initialization of the
         * {@link Node}, use the {@link UniqueFactory#initialize(PropertyContainer, Map)} method.
         *
         * @see UniqueFactory#create(Map)
         * @see UniqueFactory#initialize(PropertyContainer, Map)
         */
        @Override
        protected Node create( Map<String, Object> properties )
        {
            return graphDatabase().createNode();
        }

        /**
         * Default implementation of {@link UniqueFactory#delete(PropertyContainer)}. Invokes
         * {@link Node#delete()}.
         *
         * @see UniqueFactory#delete(PropertyContainer)
         */
        @Override
        protected void delete( Node node )
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
         * Default implementation of {@link UniqueFactory#initialize(PropertyContainer, Map)}, does nothing
         * for {@link Relationship Relationships}. Override to perform some action with the guarantee that this method
         * is only invoked for the transaction that succeeded in creating the {@link Relationship}.
         *
         * @see UniqueFactory#initialize(PropertyContainer, Map)
         * @see UniqueFactory#create(Map)
         */
        @Override
        protected void initialize( Relationship relationship, Map<String, Object> properties )
        {
            // this class has the create() method, initialize() is optional
        }

        /**
         * Default implementation of {@link UniqueFactory#delete(PropertyContainer)}. Invokes
         * {@link Relationship#delete()}.
         *
         * @see UniqueFactory#delete(PropertyContainer)
         */
        @Override
        protected void delete( Relationship relationship )
        {
            relationship.delete();
        }
    }

    /**
     * Implement this method to create the {@link Node} or {@link Relationship} to index.
     *
     * This method will be invoked exactly once per transaction that attempts to create an entry in the index.
     * The created entity might be discarded if another thread creates an entity with the same mapping concurrently.
     *
     * @param properties the properties that this entity will is to be indexed uniquely with.
     * @return the entity to add to the index.
     */
    protected abstract T create( Map<String, Object> properties );

    /**
     * Implement this method to initialize the {@link Node} or {@link Relationship} created for being stored in the index.
     *
     * This method will be invoked exactly once per created unique entity.
     *
     * The created entity might be discarded if another thread creates an entity concurrently.
     * This method will however only be invoked in the transaction that succeeds in creating the node.
     *
     * @param created the created entity to initialize.
     * @param properties the properties that this entity was indexed uniquely with.
     */
    protected abstract void initialize( T created, Map<String, Object> properties );

    /**
     * Invoked after a new entity has been {@link #create(Map) created}, but adding it to the index failed (due to being
     * added by another transaction concurrently). The purpose of this method is to undo the {@link #create(Map)
     * creation of the entity}, the default implementations of this method remove the entity. Override this method to
     * define a different behavior.
     *
     * @param created the entity that was created but was not added to the index.
     */
    protected abstract void delete( T created );

    /**
     * Get the indexed entity, creating it (exactly once) if no indexed entity exists.
     * @param key the key to find the entity under in the index.
     * @param value the value the key is mapped to for the entity in the index.
     * @return the unique entity in the index.
     */
    public final T getOrCreate( String key, Object value )
    {
        Transaction tx = graphDatabase().beginTx();
        try
        {
            T result = index.get( key, value ).getSingle();
            if ( result == null )
            {
                Map<String, Object> properties = Collections.singletonMap( key, value );
                T created = create( properties );
                result = index.putIfAbsent( created, key, value );
                if ( result == null )
                {
                    initialize( created, properties );
                    result = created;
                }
                else
                {
                    delete( created );
                }
            }
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
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
}
