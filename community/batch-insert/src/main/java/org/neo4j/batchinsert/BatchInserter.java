/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.batchinsert;

import org.neo4j.graphdb.RelationshipType;

/**
 * The batch inserter drops support for transactions and concurrency in favor
 * of insertion speed. When done using the batch inserter {@link #shutdown()}
 * must be invoked and complete successfully for the Neo4j store to be in
 * consistent state.
 * <p>
 * Only one thread at a time may work against the batch inserter, multiple
 * threads performing concurrent access have to employ synchronization.
 * <p>
 * Transactions are not supported so if the JVM/machine crashes or you fail to
 * invoke {@link #shutdown()} before JVM exits the Neo4j store can be considered
 * being in non consistent state and the insertion has to be re-done from
 * scratch.
 */
public interface BatchInserter extends AutoCloseable
{
    /**
     * Creates a node assigning next available id to id and also adds any
     * properties supplied.
     *
     * @return The id of the created node.
     */
    long createNode();

    /**
     * Sets the property with name {@code propertyName} of node with id
     * {@code node} to the value {@code propertyValue}. If the property exists
     * it is updated, otherwise created.
     *
     * @param node The node id of the node whose property is to be set
     * @param propertyName The name of the property to set
     * @param propertyValue The value of the property to set
     */
    void setNodeProperty( long node, String propertyName, Object propertyValue );

    /**
     * Sets the property with name {@code propertyName} of relationship with id
     * {@code relationship} to the value {@code propertyValue}. If the property
     * exists it is updated, otherwise created.
     *
     * @param relationship The node id of the relationship whose property is to
     *            be set
     * @param propertyName The name of the property to set
     * @param propertyValue The value of the property to set
     */
    void setRelationshipProperty( long relationship, String propertyName, Object propertyValue );

    /**
     * Creates a relationship between two nodes of a specific type.
     *
     * @param node1 the start node.
     * @param node2 the end node.
     * @param type relationship type.
     * @return the id of the created relationship.
     */
    long createRelationship( long node1, long node2, RelationshipType type );

    /**
     * Shuts down this batch inserter syncing all changes that are still only
     * in memory to disk. Failing to invoke this method may leave the Neo4j
     * store in a inconsistent state.
     *
     * Note that this method will trigger population of all indexes, both
     * those created in the batch insertion session, as well as those that existed
     * previously. This may take a long time, depending on data size.
     *
     * <p>
     * After this method has been invoked any other method call to this batch
     * inserter is illegal.
     */
    void shutdown();

    /**
     * Synonymous with {@link #shutdown()}, allowing the BatchInserter to be used in try-with-resources clauses.
     */
    @Override
    default void close()
    {
        shutdown();
    }

}
