/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.values.storable.ValueTuple;

import static org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndex.BY_VALUE_COMPARATOR;

class NativeSchemaNumberIndexUpdater<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue>
        implements IndexUpdater
{
    private static final int BATCH_SIZE = 100;

    private final KEY treeKey;
    private final VALUE treeValue;
    private final ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger;
    // having this as an array we can easily sort using Arrays.sort and don't have to pay the cost of clearing after apply
    private final IndexEntryUpdate<?>[] batchedUpdates = new IndexEntryUpdate[BATCH_SIZE];
    private Writer<KEY,VALUE> writer;
    private int batchedUpdatesCursor;

    private boolean closed = true;
    private boolean manageClosingOfWriter;

    NativeSchemaNumberIndexUpdater( KEY treeKey, VALUE treeValue )
    {
        this.treeKey = treeKey;
        this.treeValue = treeValue;
        this.conflictDetectingValueMerger = new ConflictDetectingValueMerger<>();
    }

    NativeSchemaNumberIndexUpdater<KEY,VALUE> initialize( Writer<KEY,VALUE> writer, boolean manageClosingOfWriter )
    {
        if ( !closed )
        {
            throw new IllegalStateException( "Updater still open" );
        }

        this.manageClosingOfWriter = manageClosingOfWriter;
        this.writer = writer;
        closed = false;
        return this;
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        assertOpen();
        batchedUpdates[batchedUpdatesCursor++] = update;
        if ( batchedUpdatesCursor == BATCH_SIZE )
        {
            applyBatchedUpdates();
        }
    }

    private void applyBatchedUpdates() throws IOException, IndexEntryConflictException
    {
        Arrays.sort( batchedUpdates, 0, batchedUpdatesCursor, BY_VALUE_COMPARATOR );
        for ( int i = 0; i < batchedUpdatesCursor; i++ )
        {
            processUpdate( treeKey, treeValue, batchedUpdates[i], writer, conflictDetectingValueMerger );
        }
        // efficient "clear"
        batchedUpdatesCursor = 0;
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        try
        {
            applyBatchedUpdates();
        }
        finally
        {
            if ( manageClosingOfWriter )
            {
                writer.close();
            }
            closed = true;
        }
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Updater has been closed" );
        }
    }

    static <KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> void processUpdate( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer, ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
            throws IOException, IndexEntryConflictException
    {
        switch ( update.updateMode() )
        {
        case ADDED:
            processAdd( treeKey, treeValue, update, writer, conflictDetectingValueMerger );
            break;
        case CHANGED:
            processChange( treeKey, treeValue, update, writer, conflictDetectingValueMerger );
            break;
        case REMOVED:
            processRemove( treeKey, update, writer );
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    private static <KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> void processRemove( KEY treeKey,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer ) throws IOException
    {
        // todo Do we need to verify that we actually removed something at all?
        // todo Difference between online and recovery?
        treeKey.from( update.getEntityId(), update.values() );
        writer.remove( treeKey );
    }

    private static <KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> void processChange( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer,
            ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
            throws IOException, IndexEntryConflictException
    {
        // Remove old entry
        treeKey.from( update.getEntityId(), update.beforeValues() );
        writer.remove( treeKey );
        // Insert new entry
        treeKey.from( update.getEntityId(), update.values() );
        treeValue.from( update.values() );
        writer.merge( treeKey, treeValue, conflictDetectingValueMerger );
        assertNoConflict( update, conflictDetectingValueMerger );
    }

    static <KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> void processAdd( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer,
            ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
            throws IOException, IndexEntryConflictException
    {
        treeKey.from( update.getEntityId(), update.values() );
        treeValue.from( update.values() );
        writer.merge( treeKey, treeValue, conflictDetectingValueMerger );
        assertNoConflict( update, conflictDetectingValueMerger );
    }

    private static <KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> void assertNoConflict( IndexEntryUpdate<?> update,
            ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger ) throws IndexEntryConflictException
    {
        if ( conflictDetectingValueMerger.wasConflict() )
        {
            long existingNodeId = conflictDetectingValueMerger.existingNodeId();
            long addedNodeId = conflictDetectingValueMerger.addedNodeId();
            throw new IndexEntryConflictException( existingNodeId, addedNodeId, ValueTuple.of( update.values() ) );
        }
    }
}
