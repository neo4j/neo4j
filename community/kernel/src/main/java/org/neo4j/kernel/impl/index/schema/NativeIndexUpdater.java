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

import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

class NativeIndexUpdater<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        implements IndexUpdater
{
    private final KEY treeKey;
    private final VALUE treeValue;
    private final ConflictDetectingValueMerger<KEY,VALUE,Value[]> conflictDetectingValueMerger = new ThrowingConflictDetector<>( true );
    private Writer<KEY,VALUE> writer;

    private boolean closed = true;

    NativeIndexUpdater( KEY treeKey, VALUE treeValue )
    {
        this.treeKey = treeKey;
        this.treeValue = treeValue;
    }

    NativeIndexUpdater<KEY,VALUE> initialize( Writer<KEY,VALUE> writer )
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
    public void process( IndexEntryUpdate<?> update ) throws IndexEntryConflictException
    {
        assertOpen();
        processUpdate( treeKey, treeValue, update, writer, conflictDetectingValueMerger );
    }

    @Override
    public void close()
    {
        closed = true;
        IOUtils.closeAllUnchecked( writer );
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Updater has been closed" );
        }
    }

    static <KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> void processUpdate( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer, ConflictDetectingValueMerger<KEY,VALUE,Value[]> conflictDetectingValueMerger )
            throws IndexEntryConflictException
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

    private static <KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> void processRemove( KEY treeKey,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer )
    {
        // todo Do we need to verify that we actually removed something at all?
        // todo Difference between online and recovery?
        initializeKeyFromUpdate( treeKey, update.getEntityId(), update.values() );
        writer.remove( treeKey );
    }

    private static <KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> void processChange( KEY treeKey, VALUE treeValue,
            IndexEntryUpdate<?> update, Writer<KEY,VALUE> writer,
            ConflictDetectingValueMerger<KEY,VALUE,Value[]> conflictDetectingValueMerger )
            throws IndexEntryConflictException
    {
        // Remove old entry
        initializeKeyFromUpdate( treeKey, update.getEntityId(), update.beforeValues() );
        writer.remove( treeKey );
        // Insert new entry
        initializeKeyFromUpdate( treeKey, update.getEntityId(), update.values() );
        treeValue.from( update.values() );
        conflictDetectingValueMerger.controlConflictDetection( treeKey );
        writer.merge( treeKey, treeValue, conflictDetectingValueMerger );
        conflictDetectingValueMerger.checkConflict( update.values() );
    }

    private static <KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> void processAdd( KEY treeKey, VALUE treeValue, IndexEntryUpdate<?> update,
            Writer<KEY,VALUE> writer, ConflictDetectingValueMerger<KEY,VALUE,Value[]> conflictDetectingValueMerger )
            throws IndexEntryConflictException
    {
        initializeKeyAndValueFromUpdate( treeKey, treeValue, update.getEntityId(), update.values() );
        conflictDetectingValueMerger.controlConflictDetection( treeKey );
        writer.merge( treeKey, treeValue, conflictDetectingValueMerger );
        conflictDetectingValueMerger.checkConflict( update.values() );
    }

    static <KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> void initializeKeyAndValueFromUpdate( KEY treeKey, VALUE treeValue,
            long entityId, Value[] values )
    {
        initializeKeyFromUpdate( treeKey, entityId, values );
        treeValue.from( values );
    }

    static <KEY extends NativeIndexKey<KEY>> void initializeKeyFromUpdate( KEY treeKey, long entityId, Value[] values )
    {
        treeKey.initialize( entityId );
        for ( int i = 0; i < values.length; i++ )
        {
            treeKey.initFromValue( i, values[i], NEUTRAL );
        }
    }
}
