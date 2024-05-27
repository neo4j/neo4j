/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api;

public interface PropertyDataShardingFunction {
    /**
     * @return the number of property data shards in existence.
     */
    int numShards();

    /**
     * @param nodeId the node ID from entity graph.
     * @return the shard ID that property data for {@code nodeId} is located on.
     */
    int nodeShard(long nodeId);

    /**
     * @param startNodeId start node ID of the relationship from entity graph.
     * @param relationshipId ID of the relationship from entity graph.
     * @return the shard ID that property data for {@code relationshipId} is located on.
     */
    int relationshipShard(long startNodeId, long relationshipId);

    /**
     * Translates a node ID from what it is in the entity graph into what it is in the shard it's located on.
     * @param nodeId node ID to translate.
     * @return the node ID (from entity graph) translated what that ID is on the shard it's located on.
     */
    long nodeIdTranslateEntityGraphToShard(long nodeId);

    /**
     * Translates a relationship ID from what it is in the entity graph into what it is in the shard it's located on.
     * @param relationshipId relationship ID to translate.
     * @return the relationship ID (from entity graph) translated into what that ID is on the shard it's located on.
     */
    long relationshipIdTranslateEntityGraphToShard(long relationshipId);

    /**
     * @param relationshipId the relationship ID from entity graph to get the owning node ID for.
     * @param startNodeId the start node ID of the relationship.
     * @return the node ID that is considered the owner of this relationship.
     */
    long nodeOwningRelationship(long relationshipId, long startNodeId);
}
