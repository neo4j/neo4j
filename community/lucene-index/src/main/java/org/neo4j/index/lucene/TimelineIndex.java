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
package org.neo4j.index.lucene;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;

/**
 * A utility for ordering nodes or relationships in a timeline. Entities are added
 * to the timeline and then queried given a time period, w/ or w/o lower/upper
 * bounds, for example "Give me all entities before this given timestamp" or
 * "Give me all nodes between these two timestamps".
 * 
 * Please note that the timestamps don't need to represent actual points in
 * time, any <code>long</code> that identifies the indexed {@link Node} or
 * {@link Relationship} and defines its global order is fine.
 */
public interface TimelineIndex<T extends PropertyContainer>
{
    /**
     * @return the last entity in the timeline, that is the entity with the highest
     * timestamp or {@code null} if the timeline is empty.
     */
    T getLast();

    /**
     * @return the first entity in the timeline, that is the entity with the lowest
     * timestamp or {@code null} if the timeline is empty.
     */
    T getFirst();

    /**
     * Removes an entity from the timeline. The timestamp should be the same
     * as when it was added.
     * 
     * @param entity the entity to remove from this timeline.
     * @param timestamp the timestamp this entity was added with.
     */
    void remove( T entity, long timestamp );

    /**
     * Adds an entity to this timeline with the given {@code timestamp}.
     * 
     * @param entity the entity to add to this timeline.
     * @param timestamp the timestamp to use.
     */
    void add( T entity, long timestamp );

    /**
     * Query the timeline with optional lower/upper bounds and get back
     * entities within that range, ordered by date. If {@code reversed} is
     * {@code true} the order of the result is reversed.
     * 
     * @param startTimestampOrNull the start timestamp, entities with greater
     * timestamp value will be returned (exclusive). Will be ignored if {@code null}.
     * @param endTimestampOrNull the end timestamp, entities with lesser timestamp
     * @param reversed reverses the result order if {@code true}.
     * value will be returned (exclude). Will be ignored if {@code null}.
     * @return all entities within the given boundaries in this timeline, ordered
     * by timestamp.
     */
    IndexHits<T> getBetween( Long startTimestampOrNull, Long endTimestampOrNull, boolean reversed );
    
    /**
     * Query the timeline with optional lower/upper bounds and get back
     * entities within that range, ordered by date with lowest first.
     * 
     * @param startTimestampOrNull the start timestamp, entities with greater
     * timestamp value will be returned (exclusive). Will be ignored if {@code null}.
     * @param endTimestampOrNull the end timestamp, entities with lesser timestamp
     * value will be returned (exclude). Will be ignored if {@code null}.
     * @return all entities within the given boundaries in this timeline, ordered
     * by timestamp.
     */
    IndexHits<T> getBetween( Long startTimestampOrNull, Long endTimestampOrNull );
}
