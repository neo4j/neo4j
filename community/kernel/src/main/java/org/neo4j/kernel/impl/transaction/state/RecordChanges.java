/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.statistics.IntCounter;
import org.neo4j.kernel.impl.util.statistics.LocalIntCounter;

/**
 * Manages changes to records in a transaction. Before/after state is supported as well as
 * deciding when to make a record heavy and when to consider it changed for inclusion in the
 * transaction as a command.
 *
 * @author Mattias Persson
 *
 * @param <KEY>
 * @param <RECORD>
 * @param <ADDITIONAL>
 */
public class RecordChanges<KEY,RECORD,ADDITIONAL> implements RecordAccess<KEY,RECORD,ADDITIONAL>
{
    private Map<KEY, RecordProxy<KEY,RECORD,ADDITIONAL>> recordChanges = new HashMap<>();
    private final Loader<KEY,RECORD,ADDITIONAL> loader;
    private final boolean manageBeforeState;
    private final IntCounter changeCounter;

    public RecordChanges( Loader<KEY,RECORD,ADDITIONAL> loader, boolean manageBeforeState, IntCounter globalCounter )
    {
        this.loader = loader;
        this.manageBeforeState = manageBeforeState;
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
    public RecordProxy<KEY, RECORD, ADDITIONAL> getIfLoaded( KEY key )
    {
        return recordChanges.get( key );
    }

    @Override
    public RecordProxy<KEY, RECORD, ADDITIONAL> getOrLoad( KEY key, ADDITIONAL additionalData )
    {
        RecordProxy<KEY, RECORD, ADDITIONAL> result = recordChanges.get( key );
        if ( result == null )
        {
            result = new RecordChange<>( recordChanges, changeCounter, key,
                    loader.load( key, additionalData ), loader, manageBeforeState, false, additionalData );
        }
        return result;
    }

    @Override
    public void setTo( KEY key, RECORD newRecord, ADDITIONAL additionalData )
    {
        RecordChange<KEY, RECORD, ADDITIONAL> recordChange = new RecordChange<>( recordChanges, changeCounter,
                key, newRecord, loader, manageBeforeState, false, additionalData );
        recordChanges.put( key, recordChange );
        recordChange.forChangingData();
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
            recordChanges = new HashMap<>();
        }
        changeCounter.clear();
    }

    @Override
    public RecordProxy<KEY, RECORD, ADDITIONAL> create( KEY key, ADDITIONAL additionalData )
    {
        if ( recordChanges.containsKey( key ) )
        {
            throw new IllegalStateException( key + " already exists" );
        }

        RECORD record = loader.newUnused( key, additionalData );
        RecordChange<KEY, RECORD, ADDITIONAL> change = new RecordChange<>(
                recordChanges, changeCounter, key, record, loader, manageBeforeState, true, additionalData);
        recordChanges.put( key, change );
        return change;
    }

    @Override
    public Iterable<RecordProxy<KEY,RECORD,ADDITIONAL>> changes()
    {
        return Iterables.filter( new Predicate<RecordProxy<KEY,RECORD,ADDITIONAL>>()
        {
            @Override
            public boolean test( RecordProxy<KEY, RECORD, ADDITIONAL> item )
            {
                return item.isChanged();
            }
        }, recordChanges.values() );
    }

    public static class RecordChange<KEY,RECORD,ADDITIONAL> implements RecordProxy<KEY, RECORD, ADDITIONAL>
    {
        private final Map<KEY, RecordProxy<KEY, RECORD, ADDITIONAL>> allChanges;
        private final IntCounter changeCounter;
        private final Loader<KEY,RECORD,ADDITIONAL> loader;

        private final ADDITIONAL additionalData;
        private final RECORD record;
        private final boolean created;
        private final KEY key;
        private final boolean manageBeforeState;

        private RECORD before;
        private boolean changed;

        public RecordChange(Map<KEY, RecordProxy<KEY, RECORD, ADDITIONAL>> allChanges, IntCounter changeCounter,
                            KEY key, RECORD record,
                            Loader<KEY, RECORD, ADDITIONAL> loader, boolean manageBeforeState, boolean created, ADDITIONAL additionalData)
        {
            this.allChanges = allChanges;
            this.changeCounter = changeCounter;
            this.key = key;
            this.record = record;
            this.loader = loader;
            this.manageBeforeState = manageBeforeState;
            this.created = created;
            this.additionalData = additionalData;
        }

        @Override
        public String toString()
        {
            return "RecordChange{" +
                   "record=" + record +
                   "key=" + key +
                   "created=" + created +
                   '}';
        }

        @Override
        public KEY getKey()
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
                RecordProxy<KEY, RECORD, ADDITIONAL> previous = this.allChanges.put( key, this );

                if( previous == null || !previous.isChanged() )
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
            if ( !manageBeforeState )
            {
                throw new UnsupportedOperationException( "This RecordChanges instance doesn't manage before-state" );
            }
            return before;
        }

        private void ensureHasBeforeRecordImage()
        {
            if ( manageBeforeState && this.before == null )
            {
                this.before = loader.clone( record );
            }
        }

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
