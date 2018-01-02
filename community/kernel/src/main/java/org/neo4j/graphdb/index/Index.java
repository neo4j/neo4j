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
package org.neo4j.graphdb.index;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * An index to associate key/value pairs with entities ({@link Node}s or
 * {@link Relationship}s) for fast lookup and querying. Any number of key/value
 * pairs can be associated with any number of entities using
 * {@link #add(PropertyContainer, String, Object)} and dissociated with
 * {@link #remove(PropertyContainer, String, Object)}. Querting is done using
 * {@link #get(String, Object)} for exact lookups and {@link #query(Object)} or
 * {@link #query(String, Object)} for more advanced querying, exposing querying
 * capabilities from the backend which is backing this particular index.
 *
 * Write operations participates in transactions so committing and rolling back
 * works the same way as usual in Neo4j.
 *
 * @author Mattias Persson
 *
 * @param <T> The type of entities this index manages. It may be either
 * {@link Node}s or {@link Relationship}s.
 */
public interface Index<T extends PropertyContainer> extends ReadableIndex<T>
{
    /**
     * Adds a key/value pair for {@code entity} to the index. If that key/value
     * pair for the entity is already in the index it's up to the
     * implementation to make it so that such an operation is idempotent.
     *
     * @param entity the entity (i.e {@link Node} or {@link Relationship})
     * to associate the key/value pair with.
     * @param key the key in the key/value pair to associate with the entity.
     * @param value the value in the key/value pair to associate with the
     * entity.
     */
    void add( T entity, String key, Object value );

    /**
     * Removes a key/value pair for {@code entity} from the index. If that
     * key/value pair isn't associated with {@code entity} in this index this
     * operation doesn't do anything.
     *
     * @param entity the entity (i.e {@link Node} or {@link Relationship})
     * to dissociate the key/value pair from.
     * @param key the key in the key/value pair to dissociate from the entity.
     * @param value the value in the key/value pair to dissociate from the
     * entity.
     */
    void remove( T entity, String key, Object value );

    /**
     * Removes key/value pairs for {@code entity} where key is {@code key}
     * from the index.
     *
     * Implementations can choose to not implement this method and should
     * in that case throw {@link UnsupportedOperationException}.
     *
     * @param entity the entity ({@link Node} or {@link Relationship}) to
     * remove the this index.
     * @param key the key associated with the index entry
     */
    void remove( T entity, String key );

    /**
     * Removes an entity from the index and all its key/value pairs which
     * has been previously associated using
     * {@link #add(PropertyContainer, String, Object)}.
     *
     * Implementations can choose to not implement this method and should
     * in that case throw {@link UnsupportedOperationException}.
     *
     * @param entity the entity ({@link Node} or {@link Relationship}) to
     * remove the this index.
     */
    void remove( T entity );

    /**
     * Clears the index and deletes the configuration associated with it. After
     * this it's invalid to call any other method on this index. However if the
     * transaction which the delete operation was called in gets rolled back
     * it again becomes ok to use this index.
     */
    void delete();

    /**
     * Add the entity to this index for the given key/value pair if this particular
     * key/value pair doesn't already exist.
     * 
     * This ensures that only one entity will be associated with the key/value pair
     * even if multiple transactions are trying to add it at the same time. One of those
     * transactions will win and add it while the others will block, waiting for the
     * winning transaction to finish. If the winning transaction was successful these
     * other transactions will return the associated entity instead of adding it.
     * If it wasn't successful the waiting transactions will begin a new race to add it.
     *
     * @param entity the entity (i.e {@link Node} or {@link Relationship})
     * to associate the key/value pair with.
     * @param key the key in the key/value pair to associate with the entity.
     * @param value the value in the key/value pair to associate with the
     * entity.
     * @return the previously indexed entity, or {@code null} if no entity was
     * indexed before (and the specified entity was added to the index).
     */
    T putIfAbsent( T entity, String key, Object value );
}
