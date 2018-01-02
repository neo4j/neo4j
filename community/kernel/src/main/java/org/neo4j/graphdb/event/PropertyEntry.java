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
package org.neo4j.graphdb.event;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * Represents a changed property.
 *
 * Instances of this interface represent the property as it is after the
 * transaction when returned from
 * {@link TransactionData#assignedNodeProperties()} or
 * {@link TransactionData#assignedRelationshipProperties()}. Instances of this
 * interface represent the property as it was before the transaction as well as
 * how it will be after the transaction has been committed.
 *
 * @author Tobias Ivarsson
 *
 * @param <T> The type of the entity the property belongs to, either
 *            {@link Node} or {@link Relationship}.
 */
public interface PropertyEntry<T extends PropertyContainer>
{
    /**
     * Get the entity that this property was modified on. The entity is either a
     * {@link Node} or a {@link Relationship}, depending on the generic type of
     * this instance.
     *
     * @return the {@link Node} or {@link Relationship} that the property was
     *         modified on.
     */
    T entity();

    /**
     * Get the key of the modified property.
     *
     * @return the key of the modified property.
     */
    String key();
    
    /**
     * Get the value of the modified property as it was before the transaction
     * (which modified it) started. If this {@link PropertyEntry} was returned
     * from {@link TransactionData#assignedNodeProperties()} or
     * {@link TransactionData#assignedRelationshipProperties()}, the value
     * returned from this method is the value that was set for {@code key} on
     * {@code entity} before the transaction started, or {@code null} if such a
     * property wasn't set.
     * 
     * If this {@link PropertyEntry} was returned from
     * {@link TransactionData#removedNodeProperties()} or
     * {@link TransactionData#removedRelationshipProperties()} the value
     * returned from this method is the value that was stored at this property
     * before the transaction started.
     * 
     * @return The value of the property as it was before the transaction
     * started.
     */
    Object previouslyCommitedValue();

    /**
     * Get the value of the modified property. If this {@link PropertyEntry}
     * was returned from {@link TransactionData#assignedNodeProperties()} or
     * {@link TransactionData#assignedRelationshipProperties()}, the value
     * returned from this method is the value that will be assigned to the
     * property after the transaction is committed. If this
     * {@link PropertyEntry} was returned from
     * {@link TransactionData#removedNodeProperties()} or
     * {@link TransactionData#removedRelationshipProperties()} an
     * {@link IllegalStateException} will be thrown.
     * 
     * @return The value of the modified property.
     * @throws IllegalStateException if this method is called where this
     * instance represents a removed property.
     */
    Object value();
}
