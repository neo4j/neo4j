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

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.values.ValueTuple;

class NativeSchemaNumberIndexUpdater<KEY extends NumberKey, VALUE extends NumberValue>
        implements IndexUpdater
{
    private final KEY treeKey;
    private final VALUE treeValue;
    private final ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger;
    private Writer<KEY,VALUE> writer;

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
    public void process( IndexEntryUpdate update ) throws IOException, IndexEntryConflictException
    {
        assertOpen();
        switch ( update.updateMode() )
        {
        case ADDED:
            processAdd( treeKey, treeValue, update, writer, conflictDetectingValueMerger );
            break;
        case CHANGED:
            processChange( treeKey, treeValue, update, writer, conflictDetectingValueMerger );
            break;
        case REMOVED:
            throw new UnsupportedOperationException( "Implement me" );
        default:
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        if ( manageClosingOfWriter )
        {
            writer.close();
        }
        closed = true;
    }

    @Override
    public void remove( PrimitiveLongSet nodeIds ) throws IOException
    {
        assertOpen();
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Updater has been closed" );
        }
    }

    private static <KEY extends NumberKey, VALUE extends NumberValue> void processChange( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate update, Writer<KEY,VALUE> singleWriter,
            ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
            throws IOException, IndexEntryConflictException
    {
        // Remove old entry
        treeKey.from( update.getEntityId(), update.beforeValues() );
        singleWriter.remove( treeKey );
        // Insert new entrty
        treeKey.from( update.getEntityId(), update.values() );
        treeValue.from( update.values() );
        singleWriter.merge( treeKey, treeValue, conflictDetectingValueMerger );
        assertNoConflict( update, conflictDetectingValueMerger );
    }

    static <KEY extends NumberKey, VALUE extends NumberValue> void processAdd( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate update, Writer<KEY,VALUE> singleWriter,
            ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
            throws IOException, IndexEntryConflictException
    {
        treeKey.from( update.getEntityId(), update.values() );
        treeValue.from( update.values() );
        singleWriter.merge( treeKey, treeValue, conflictDetectingValueMerger );
        assertNoConflict( update, conflictDetectingValueMerger );
    }

    private static <KEY extends NumberKey, VALUE extends NumberValue> void assertNoConflict( IndexEntryUpdate update,
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
