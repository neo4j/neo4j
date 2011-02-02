/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;

/**
 * A utility for ordering nodes in a timeline. You add nodes to the timeline and
 * then you can query for nodes given a time period, w/ or w/o lower/upper
 * bounds, for example "Give me all nodes before this given timestamp" or
 * "Give me all nodes between these two timestamps".
 * 
 * Please note that the timestamps don't need to represent actual points in
 * time, any <code>long</code> that identifies the indexed {@link Node} and
 * defines its global order is fine.
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
     * Removes a node from the timeline. It will throw an exception if {@code
     * nodeToRemove} isn't added in this timeline.
     * 
     * @param nodeToRemove the node to remove from this timeline
     * @throws IllegalArgumentException if {@code null} node or node not
     *             connected to this timeline.
     */
//    void removeNode( Node nodeToRemove );
    
    void remove( T entity, long timestamp );

    /**
     * Adds a node to this timeline with the given {@code timestamp}.
     * 
     * @param nodeToAdd the node to add to this timeline.
     * @param timestamp the timestamp to use
     * @throws IllegalArgumentException if already added to this timeline or
     *             {@code null} node.
     */
    void add( T entity, long timestamp );

    /**
     * Returns nodes which were added with the given {@code timestamp}.
     * 
     * @param timestamp the timestamp to get nodes for.
     * @return nodes which were added with the given {@code timestamp}.
     */
//    Iterable<Node> getNodes( long timestamp );

    /**
     * Returns all added nodes in this timeline ordered by increasing timestamp.
     * 
     * @return all the nodes in the timeline ordered by increasing timestamp.
     */
//    Iterable<Node> getAllNodes();

    /**
     * Returns all the nodes after (exclusive) {@code timestamp} ordered by
     * increasing timestamp.
     * 
     * @param timestamp the timestamp value, nodes with greater timestamp value
     *            will be returned.
     * @return all nodes after (exclusive) {@code timestamp} ordered by
     *         increasing timestamp.
     */
//    Iterable<Node> getAllNodesAfter( long timestamp );

    /**
     * Returns all the nodes before (exclusive) {@code timestamp} ordered by
     * increasing timestamp.
     * 
     * @param timestamp the timestamp value, nodes with lesser timestamp value
     *            will be returned.
     * @return all nodes before (exclusive) {@code timestamp} ordered by
     *         increasing timestamp.
     */
//    Iterable<Node> getAllNodesBefore( long timestamp );

    /**
     * Returns all the nodes after (exclusive) {@code afterTimestamp} and before
     * (exclusive) {@code beforeTimestamp} ordered by increasing timestamp.
     * 
     * @param startTimestamp the start timestamp, nodes with greater timestamp
     *            value will be returned.
     * @param endTimestamp the end timestamp, nodes with lesser timestamp value
     *            will be returned.
     * @return all nodes between (exclusive) the specified timestamps ordered by
     *         increasing timestamp.
     */
//    Iterable<Node> getAllNodesBetween( long startTimestamp, long endTimestamp );

    /**
     * Convenience method which you can use {@link #getAllNodes()},
     * {@link #getAllNodesAfter(long)}, {@link #getAllNodesBefore(long)} and
     * {@link #getAllNodesBetween(long, long)} in a single method.
     * 
     * @param startTimestampOrNull the start timestamp, nodes with greater
     *            timestamp value will be returned. Will be ignored if {@code
     *            null}.
     * @param endTimestampOrNull the end timestamp, nodes with lesser timestamp
     *            value will be returned. Will be ignored if {@code null}.
     * @return all nodes in this timeline ordered by timestamp. A range can be
     *         given with the {@code startTimestampOrNull} and/or {@code
     *         endTimestampOrNull} (where {@code null} means no restriction).
     */
    Iterable<T> getBetween( Long startTimestampOrNull, Long endTimestampOrNull, boolean reversed );
    
    Iterable<T> getBetween( Long startTimestampOrNull, Long endTimestampOrNull );
    
    /**
     * Will return the timestamp for {@code node} if it has been added to this
     * timeline. If {@code node} hasn't been added to this timeline a runtime
     * exception will be thrown.
     * 
     * @param node the node to return the timestamp for.
     * @return the timestamp for {@code node}.
     */
//    long getTimestampForNode( Node node );
}
