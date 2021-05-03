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
package org.neo4j.internal.recordstorage;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;

import java.util.Collection;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.LocalIntCounter;

import static org.neo4j.collection.trackable.HeapTrackingCollections.newLongObjectMap;

/**
 * Manages changes to records in a transaction. Before/after state is supported as well as
 * deciding when to make a record heavy and when to consider it changed for inclusion in the
 * transaction as a command.
 *
 * @param <RECORD> type of record
 * @param <ADDITIONAL> additional payload
 */
public class RecordChanges<RECORD extends AbstractBaseRecord,ADDITIONAL> implements RecordAccess<RECORD,ADDITIONAL>
{
    private final MutableLongObjectMap<RecordProxy<RECORD, ADDITIONAL>> recordChanges;
    private final Loader<RECORD,ADDITIONAL> loader;
    private final MutableInt changeCounter;
    private final LoadMonitor loadMonitor;

    public RecordChanges( Loader<RECORD,ADDITIONAL> loader, MutableInt globalCounter, MemoryTracker memoryTracker, LoadMonitor loadMonitor )
    {
        this.loader = loader;
        this.recordChanges = newLongObjectMap( memoryTracker );
        this.changeCounter = new LocalIntCounter( globalCounter );
        this.loadMonitor = loadMonitor;
    }

    @Override
    public String toString()
    {
        return "RecordChanges{" +
               "recordChanges=" + recordChanges +
               '}';
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getIfLoaded( long key )
    {
        return recordChanges.get( key );
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getOrLoad( long key, ADDITIONAL additionalData, RecordLoad load, CursorContext cursorContext )
    {
        RecordProxy<RECORD, ADDITIONAL> result = recordChanges.get( key );
        if ( result == null )
        {
            RECORD record = loader.load( key, additionalData, load, cursorContext );
            result = new RecordChange<>( recordChanges, changeCounter, key, record, loader, false, additionalData, loadMonitor, cursorContext );
        }
        return result;
    }

    @Override
    public RecordProxy<RECORD,ADDITIONAL> setRecord( long key, RECORD record, ADDITIONAL additionalData, CursorContext cursorContext )
    {
        RecordChange<RECORD, ADDITIONAL> recordChange =
                new RecordChange<>( recordChanges, changeCounter, key, record, loader, false, additionalData, loadMonitor, cursorContext );
        recordChanges.put( key, recordChange );
        return recordChange;
    }

    @Override
    public int changeSize()
    {
        return changeCounter.intValue();
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> create( long key, ADDITIONAL additionalData, CursorContext cursorContext )
    {
        if ( recordChanges.containsKey( key ) )
        {
            throw new IllegalStateException( key + " already exists" );
        }

        RECORD record = loader.newUnused( key, additionalData );
        RecordChange<RECORD,ADDITIONAL> change =
                new RecordChange<>( recordChanges, changeCounter, key, record, loader, true, additionalData, loadMonitor, cursorContext );
        recordChanges.put( key, change );
        return change;
    }

    @Override
    public Collection<RecordProxy<RECORD,ADDITIONAL>> changes()
    {
        return recordChanges.values();
    }

    public static class RecordChange<RECORD extends AbstractBaseRecord,ADDITIONAL> implements RecordProxy<RECORD, ADDITIONAL>
    {
        private final MutableLongObjectMap<RecordProxy<RECORD, ADDITIONAL>> allChanges;
        private final MutableInt changeCounter;
        private final Loader<RECORD,ADDITIONAL> loader;

        private final ADDITIONAL additionalData;
        private final LoadMonitor loadMonitor;
        private final CursorContext cursorContext;
        private final RECORD record;
        private final boolean created;
        private final long key;

        private RECORD before;
        private boolean changed;

        public RecordChange( MutableLongObjectMap<RecordProxy<RECORD, ADDITIONAL>> allChanges, MutableInt changeCounter,
                long key, RECORD record, Loader<RECORD,ADDITIONAL> loader, boolean created, ADDITIONAL additionalData, LoadMonitor loadMonitor,
                CursorContext cursorContext )
        {
            this.allChanges = allChanges;
            this.changeCounter = changeCounter;
            this.key = key;
            this.record = record;
            this.loader = loader;
            this.created = created;
            this.additionalData = additionalData;
            this.loadMonitor = loadMonitor;
            this.cursorContext = cursorContext;
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
            ensureHeavy( cursorContext );
            return prepareForChange();
        }

        private RECORD prepareForChange()
        {
            ensureHasBeforeRecordImage();
            if ( !this.changed )
            {
                RecordProxy<RECORD,ADDITIONAL> previous = this.allChanges.put( key, this );

                if ( previous == null || !previous.isChanged() )
                {
                    loadMonitor.markedAsChanged( before );
                    changeCounter.increment();
                }

                this.changed = true;
            }
            return this.record;
        }

        private void ensureHeavy( CursorContext cursorContext )
        {
            if ( !created )
            {
                loader.ensureHeavy( record, cursorContext );
                if ( before != null )
                {
                    loader.ensureHeavy( before, cursorContext );
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
            ensureHeavy( cursorContext );
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

        @Override
        public ADDITIONAL getAdditionalData()
        {
            return additionalData;
        }
    }
}
