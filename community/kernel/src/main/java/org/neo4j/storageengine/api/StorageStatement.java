/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.storageengine.api;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * A statement for accessing data from a {@link StoreReadLayer}. Most data about the entities of a graph
 * are accessed through this statement interface as opposed to through the {@link StoreReadLayer} directly.
 * One of the main reasons is that the access methods returns objects, like {@link Cursor cursors} which
 * are valuable to reuse over a reasonably large window to reduce garbage churn in general.
 * <p>
 * A {@link StorageStatement} must be {@link #acquire() acquired} before use. After use the statement
 * should be {@link #release() released}. After released the statement can be acquired again.
 * Creating and closing {@link StorageStatement} and there's also benefits keeping these statements opened
 * during a longer period of time, with the assumption that it's still one thread at a time using each.
 * With that in mind these statements should not be opened and closed for each operation, perhaps not even
 * for each transaction.
 * <p>
 * All cursors provided by this statement are views over data in the store. They do not interact with transaction
 * state.
 */
public interface StorageStatement extends AutoCloseable
{
    /**
     * Acquires this statement so that it can be used, should later be {@link #release() released}.
     * Since a {@link StorageStatement} can be reused after {@link #release() released}, this call should
     * do initialization/clearing of state whereas data structures can be kept between uses.
     */
    void acquire();

    /**
     * Releases this statement so that it can later be {@link #acquire() acquired} again.
     */
    void release();

    /**
     * Closes this statement so that it can no longer be used nor {@link #acquire() acquired}.
     */
    @Override
    void close();

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link NodeItem} for selected nodes.
     * No node is selected when this method returns, a call to {@link Cursor#next()} will have to be made
     * to place the cursor over the first item and then more calls to move the cursor through the selection.
     *
     * @param nodeId id of node to get cursor for.
     * @return a {@link Cursor} over {@link NodeItem} for the given {@code nodeId}.
     */
    Cursor<NodeItem> acquireSingleNodeCursor( long nodeId );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link NodeItem} for selected nodes.
     * No node is selected when this method returns, a call to {@link Cursor#next()} will have to be made
     * to place the cursor over the first item and then more calls to move the cursor through the selection.
     *
     * @param nodeIds ids of nodes to get cursor for.
     * @return a {@link Cursor} over {@link NodeItem} for the given node ids.
     */
    Cursor<NodeItem> acquireIteratorNodeCursor( PrimitiveLongIterator nodeIds );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @param relationshipId id of relationship to get cursor for.
     * @return a {@link Cursor} over {@link RelationshipItem} for the given {@code relationshipId}.
     */
    Cursor<RelationshipItem> acquireSingleRelationshipCursor( long relationshipId );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @param relationshipIds ids of relationships to get cursor for.
     * @return a {@link Cursor} over {@link RelationshipItem} for the given relationship ids.
     */
    Cursor<RelationshipItem> acquireIteratorRelationshipCursor( PrimitiveLongIterator relationshipIds );

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link NodeItem} for selected nodes.
     * No node is selected when this method returns, a call to {@link Cursor#next()} will have to be made
     * to place the cursor over the first item and then more calls to move the cursor through the selection.
     *
     * @return {@link Cursor} over all stored nodes.
     */
    Cursor<NodeItem> nodesGetAllCursor();

    /**
     * Acquires {@link Cursor} capable of {@link Cursor#get() serving} {@link RelationshipItem} for selected
     * relationships. No relationship is selected when this method returns, a call to {@link Cursor#next()}
     * will have to be made to place the cursor over the first item and then more calls to move the cursor
     * through the selection.
     *
     * @return a {@link Cursor} over all stored relationships.
     */
    Cursor<RelationshipItem> relationshipsGetAllCursor();

    /**
     * @return {@link LabelScanReader} capable of reading nodes for specific label ids.
     */
    LabelScanReader getLabelScanReader();

    /**
     * Returns an {@link IndexReader} for searching entity ids given property values. One reader is allocated
     * and kept per index throughout the life of a statement, making the returned reader repeatable-read isolation.
     * <p>
     * <b>NOTE:</b>
     * Reader returned from this method should not be closed. All such readers will be closed during {@link #close()}
     * of the current statement.
     *
     * @param index {@link IndexDescriptor} to get reader for.
     * @return {@link IndexReader} capable of searching entity ids given property values.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    IndexReader getIndexReader( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Returns an {@link IndexReader} for searching entity ids given property values. A new reader is allocated
     * every call to this method, which means that newly committed data since the last call to this method
     * will be visible in the returned reader.
     * <p>
     * <b>NOTE:</b>
     * It is caller's responsibility to close the returned reader.
     *
     * @param index {@link IndexDescriptor} to get reader for.
     * @return {@link IndexReader} capable of searching entity ids given property values.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    IndexReader getFreshIndexReader( IndexDescriptor index ) throws IndexNotFoundKernelException;
}
