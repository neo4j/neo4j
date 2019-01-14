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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.statistics.IntCounter;
import org.neo4j.kernel.impl.util.statistics.LocalIntCounter;

/**
 * Manages changes to records in a transaction. Before/after state is supported as well as
 * deciding when to make a record heavy and when to consider it changed for inclusion in the
 * transaction as a command.
 *
 * @param <RECORD> type of record
 * @param <ADDITIONAL> additional payload
 */
public class RecordChanges<RECORD,ADDITIONAL> implements RecordAccess<RECORD,ADDITIONAL>
{
    private PrimitiveLongObjectMap<RecordProxy<RECORD,ADDITIONAL>> recordChanges = Primitive.longObjectMap();
    private final Loader<RECORD,ADDITIONAL> loader;
    private final IntCounter changeCounter;

    public RecordChanges( Loader<RECORD,ADDITIONAL> loader, IntCounter globalCounter )
    {
        this.loader = loader;
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
    public RecordProxy<RECORD, ADDITIONAL> getIfLoaded( long key )
    {
        return recordChanges.get( key );
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getOrLoad( long key, ADDITIONAL additionalData )
    {
        RecordProxy<RECORD, ADDITIONAL> result = recordChanges.get( key );
        if ( result == null )
        {
            RECORD record = loader.load( key, additionalData );
            result = new RecordChange<>( recordChanges, changeCounter, key, record, loader, false, additionalData );
        }
        return result;
    }

    @Override
    public void setTo( long key, RECORD newRecord, ADDITIONAL additionalData )
    {
        setRecord( key, newRecord, additionalData );
    }

    @Override
    public RecordProxy<RECORD,ADDITIONAL> setRecord( long key, RECORD record, ADDITIONAL additionalData )
    {
        RecordChange<RECORD, ADDITIONAL> recordChange =
                new RecordChange<>( recordChanges, changeCounter, key, record, loader, false, additionalData );
        recordChanges.put( key, recordChange );
        return recordChange;
    }

    @Override
    public int changeSize()
    {
        return changeCounter.value();
    }

    @Override
    public void close()
    {
        if ( recordChanges.size() <= 32 )
        {
            recordChanges.clear();
        }
        else
        {
            // Let's not allow the internal maps to grow too big over time.
            recordChanges = Primitive.longObjectMap();
        }
        changeCounter.clear();
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> create( long key, ADDITIONAL additionalData )
    {
        if ( recordChanges.containsKey( key ) )
        {
            throw new IllegalStateException( key + " already exists" );
        }

        RECORD record = loader.newUnused( key, additionalData );
        RecordChange<RECORD,ADDITIONAL> change =
                new RecordChange<>( recordChanges, changeCounter, key, record, loader, true, additionalData );
        recordChanges.put( key, change );
        return change;
    }

    @Override
    public Iterable<RecordProxy<RECORD,ADDITIONAL>> changes()
    {
        return Iterables.filter( RecordProxy::isChanged, recordChanges.values() );
    }

    public static class RecordChange<RECORD,ADDITIONAL> implements RecordProxy<RECORD, ADDITIONAL>
    {
        private final PrimitiveLongObjectMap<RecordProxy<RECORD,ADDITIONAL>> allChanges;
        private final IntCounter changeCounter;
        private final Loader<RECORD,ADDITIONAL> loader;

        private final ADDITIONAL additionalData;
        private final RECORD record;
        private final boolean created;
        private final long key;

        private RECORD before;
        private boolean changed;

        public RecordChange( PrimitiveLongObjectMap<RecordProxy<RECORD,ADDITIONAL>> allChanges, IntCounter changeCounter,
                long key, RECORD record, Loader<RECORD,ADDITIONAL> loader, boolean created, ADDITIONAL additionalData )
        {
            this.allChanges = allChanges;
            this.changeCounter = changeCounter;
            this.key = key;
            this.record = record;
            this.loader = loader;
            this.created = created;
            this.additionalData = additionalData;
        }

        @Override
        public String toString()
        {
            return "RecordChange{" + "record=" + record + "key=" + key + "created=" + created + '}';
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
            ensureHeavy();
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
                    changeCounter.increment();
                }

                this.changed = true;
            }
            return this.record;
        }

        private void ensureHeavy()
        {
            if ( !created )
            {
                loader.ensureHeavy( record );
                if ( before != null )
                {
                    loader.ensureHeavy( before );
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
            ensureHeavy();
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
                this.before = loader.clone( record );
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
