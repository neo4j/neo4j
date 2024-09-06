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
package org.neo4j.storageengine.api;

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * A context which {@link StorageEngine} hands out to clients and which gets passed back in
 * to calls about creating commands. One of its purposes is to reserve and release ids. E.g. internal nodes and relationship references
 * are publicly exposed even before committed, which means that they will have to be reserved before committing.
 */
public interface CommandCreationContext
        extends KernelVersionProvider, NodeIdAllocator, RelationshipIdAllocator, AutoCloseable {
    LongSupplier NO_STARTTIME_OF_OLDEST_TRANSACTION = () -> 0L;

    /**
     * Reserves a node id for future use to store a node. The reason for it being exposed here is that
     * internal ids of nodes and relationships are publicly accessible all the way out to the user.
     * This will likely change in the future though.
     *
     * @return a reserved node id for future use.
     */
    @Override
    long reserveNode();

    /**
     * Reserves a relationship id for future use to store a relationship. The reason for it being exposed here is that
     * internal ids of nodes and relationships are publicly accessible all the way out to the user.
     * This will likely change in the future though.
     *
     * @return a reserved relationship id for future use.
     * @param sourceNode id of the source node to reserve this id for.
     * @param targetNode id of the target node to reserve this id for.
     * @param relationshipType id of the relationship to reserve this id for.
     * @param sourceNodeAddedInTx whether the {@code sourceNode} is a node that is added in this transaction.
     * @param targetNodeAddedInTx whether the {@code targetNode} is a node that is added in this transaction.
     */
    @Override
    long reserveRelationship(
            long sourceNode,
            long targetNode,
            int relationshipType,
            boolean sourceNodeAddedInTx,
            boolean targetNodeAddedInTx);

    /**
     * Reserves an id for a schema record, be it for a constraint or an index, for future use to store a schema record. The reason for it being exposed here
     * is that the record ids are used for producing unique names for indexes, which we would like to do before we get to the prepare phase
     *
     * @return a reserved schema record id for future use.
     */
    long reserveSchema();

    /**
     * Reserves a label token id.
     * @return a unique label token id.
     */
    int reserveLabelTokenId();

    /**
     * Reserves a property key token id.
     * @return a unique property key token id.
     */
    int reservePropertyKeyTokenId();

    /**
     * Reserves a relationship type token id.
     * @return a unique relationship type token id.
     */
    int reserveRelationshipTypeTokenId();

    @Override
    void close();

    /**
     * Initialise command creation context for specific transactional cursor context
     * @param kernelVersionProvider provider of the kernel version for which the commands should be created
     * @param cursorContext transaction cursor context
     * @param storeCursors store cursors
     * @param startTimeOfOldestExecutingTransaction supplier to retrieve timestamp of oldest currently active transaction
     * @param locks access to locks that might be needed in implementation
     * @param lockTracer Lock tracer to use if locks are taken
     */
    void initialize(
            KernelVersionProvider kernelVersionProvider,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            LongSupplier startTimeOfOldestExecutingTransaction,
            ResourceLocker locks,
            Supplier<LockTracer> lockTracer);

    boolean resetIds();
}
