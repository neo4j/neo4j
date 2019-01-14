/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;

import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;

class NativeSchemaIndexUpdater<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue>
        implements IndexUpdater
{
    private final KEY treeKey;
    private final VALUE treeValue;
    private final ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger = new ConflictDetectingValueMerger<>( true );
    private Writer<KEY,VALUE> writer;

    private boolean closed = true;

    NativeSchemaIndexUpdater( KEY treeKey, VALUE treeValue )
    {
        this.treeKey = treeKey;
        this.treeValue = treeValue;
    }

    NativeSchemaIndexUpdater<KEY,VALUE> initialize( Writer<KEY,VALUE> writer )
    {
        if ( !closed )
        {
            throw new IllegalStateException( "Updater still open" );
        }

        this.writer = writer;
        closed = false;
        return this;
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        assertOpen();
        processUpdate( treeKey, treeValue, update, writer, conflictDetectingValueMerger );
    }

    @Override
    public void close() throws IOException
    {
        writer.close();
        closed = true;
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Updater has been closed" );
        }
    }

    static <KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> void processUpdate( KEY treeKey, VALUE treeValue,
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

    private static <KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> void processRemove( KEY treeKey,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer ) throws IOException
    {
        // todo Do we need to verify that we actually removed something at all?
        // todo Difference between online and recovery?
        treeKey.from( update.getEntityId(), update.values() );
        writer.remove( treeKey );
    }

    private static <KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> void processChange( KEY treeKey, VALUE treeValue,
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
        conflictDetectingValueMerger.controlConflictDetection( treeKey );
        writer.merge( treeKey, treeValue, conflictDetectingValueMerger );
        conflictDetectingValueMerger.checkConflict( update.values() );
    }

    static <KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> void processAdd( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer,
            ConflictDetectingValueMerger<KEY,VALUE> conflictDetectingValueMerger )
            throws IOException, IndexEntryConflictException
    {
        treeKey.from( update.getEntityId(), update.values() );
        treeValue.from( update.values() );
        conflictDetectingValueMerger.controlConflictDetection( treeKey );
        writer.merge( treeKey, treeValue, conflictDetectingValueMerger );
        conflictDetectingValueMerger.checkConflict( update.values() );
    }
}
