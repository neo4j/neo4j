/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;

import java.util.Collection;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.LocalIntCounter;

import static org.neo4j.collection.trackable.HeapTrackingCollections.newLongObjectMap;

/**
 * Manages changes to records in a transaction. Before/after state is supported as well as
 * deciding when to make a record heavy and when to consider it changed for inclusion in the
 * transaction as a command.
 *
 * @param <RECORD> type of record
 */
public class RecordChanges<RECORD> implements RecordAccess<RECORD>
{
    private final MutableLongObjectMap<RecordProxy<RECORD>> recordChanges;
    private final Loader<RECORD> loader;
    private final MutableInt changeCounter;

    public RecordChanges( Loader<RECORD> loader, MutableInt globalCounter, MemoryTracker memoryTracker )
    {
        this.loader = loader;
        this.recordChanges = newLongObjectMap( memoryTracker );
        this.changeCounter = new LocalIntCounter( globalCounter );
    }

    @Override
    public String toString()
    {
        return "RecordChanges{" +
               "recordChanges=" + recordChanges +
               '}';
    }

    @Override
    public RecordProxy<RECORD> getIfLoaded( long key )
    {
        return recordChanges.get( key );
    }

    @Override
    public RecordProxy<RECORD> getOrLoad( long key, PageCursorTracer cursorTracer )
    {
        RecordProxy<RECORD> result = recordChanges.get( key );
        if ( result == null )
        {
            RECORD record = loader.load( key, cursorTracer );
            result = new RecordChange<>( recordChanges, changeCounter, key, record, loader, false, cursorTracer );
        }
        return result;
    }

    @Override
    public RecordProxy<RECORD> setRecord( long key, RECORD record, PageCursorTracer cursorTracer )
    {
        RecordChange<RECORD> recordChange = new RecordChange<>( recordChanges, changeCounter, key, record, loader, false, cursorTracer );
        recordChanges.put( key, recordChange );
        return recordChange;
    }

    @Override
    public int changeSize()
    {
        return changeCounter.intValue();
    }

    @Override
    public RecordProxy<RECORD> create( long key, PageCursorTracer cursorTracer )
    {
        if ( recordChanges.containsKey( key ) )
        {
            throw new IllegalStateException( key + " already exists" );
        }

        RECORD record = loader.newUnused( key );
        RecordChange<RECORD> change = new RecordChange<>( recordChanges, changeCounter, key, record, loader, true, cursorTracer );
        recordChanges.put( key, change );
        return change;
    }

    @Override
    public Collection<RecordProxy<RECORD>> changes()
    {
        return recordChanges.values();
    }

    public static class RecordChange<RECORD> implements RecordProxy<RECORD>
    {
        private final MutableLongObjectMap<RecordProxy<RECORD>> allChanges;
        private final MutableInt changeCounter;
        private final Loader<RECORD> loader;

        private final PageCursorTracer cursorTracer;
        private final RECORD record;
        private final boolean created;
        private final long key;

        private RECORD before;
        private boolean changed;

        public RecordChange( MutableLongObjectMap<RecordProxy<RECORD>> allChanges, MutableInt changeCounter,
                long key, RECORD record, Loader<RECORD> loader, boolean created, PageCursorTracer cursorTracer )
        {
            this.allChanges = allChanges;
            this.changeCounter = changeCounter;
            this.key = key;
            this.record = record;
            this.loader = loader;
            this.created = created;
            this.cursorTracer = cursorTracer;
        }

        @Override
        public String toString()
        {
            return "RecordChange{record=" + record + ",key=" + key + ",created=" + created + '}';
        }

        @Override
        public long getKey()
        {
            return key;
        }

        @Override
        public RECORD forChangingLinkage()
        {
            return prepareForChange();
        }

        @Override
        public RECORD forChangingData()
        {
            ensureHeavy( cursorTracer );
            return prepareForChange();
        }

        private RECORD prepareForChange()
        {
            ensureHasBeforeRecordImage();
            if ( !this.changed )
            {
                RecordProxy<RECORD> previous = this.allChanges.put( key, this );

                if ( previous == null || !previous.isChanged() )
                {
                    changeCounter.increment();
                }

                this.changed = true;
            }
            return this.record;
        }

        private void ensureHeavy( PageCursorTracer cursorTracer )
        {
            if ( !created )
            {
                loader.ensureHeavy( record, cursorTracer );
                if ( before != null )
                {
                    loader.ensureHeavy( before, cursorTracer );
                }
            }
        }

        @Override
        public RECORD forReadingLinkage()
        {
            return this.record;
        }

        @Override
        public RECORD forReadingData()
        {
            ensureHeavy( cursorTracer );
            return this.record;
        }

        @Override
        public boolean isChanged()
        {
            return this.changed;
        }

        @Override
        public RECORD getBefore()
        {
            ensureHasBeforeRecordImage();
            return before;
        }

        private void ensureHasBeforeRecordImage()
        {
            if ( before == null )
            {
                this.before = loader.copy( record );
            }
        }

        @Override
        public boolean isCreated()
        {
            return created;
        }
    }
}
