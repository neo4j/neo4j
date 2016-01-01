/**
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.neo4j.helpers.collection.IteratorUtil.count;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;

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
    private final Map<KEY, RecordChange<KEY,RECORD,ADDITIONAL>> recordChanges = new HashMap<>();
    private final Loader<KEY,RECORD,ADDITIONAL> loader;
    private final boolean manageBeforeState;

    public RecordChanges( Loader<KEY,RECORD,ADDITIONAL> loader, boolean manageBeforeState )
    {
        this.loader = loader;
        this.manageBeforeState = manageBeforeState;
    }

    public RecordChange<KEY, RECORD, ADDITIONAL> getIfLoaded( KEY key )
    {
        return recordChanges.get( key );
    }

    @Override
    public RecordChange<KEY, RECORD, ADDITIONAL> getOrLoad( KEY key, ADDITIONAL additionalData )
    {
        RecordChange<KEY, RECORD, ADDITIONAL> result = recordChanges.get( key );
        if ( result == null )
        {
            result = new RecordChange<>( recordChanges, key,
                    loader.load( key, additionalData ), loader, manageBeforeState, false, additionalData );
        }
        return result;
    }

    public void setTo( KEY key, RECORD newRecord, ADDITIONAL additionalData )
    {
        RecordChange<KEY, RECORD, ADDITIONAL> recordChange = new RecordChange<>( recordChanges, key, newRecord, loader,
                manageBeforeState, false, additionalData );
        recordChanges.put( key, recordChange );
        recordChange.forChangingData();
    }

    public int changeSize()
    {
        // TODO optimize?
        return count( changes() );
    }

    @Override
    public void close()
    {
        recordChanges.clear();
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
                recordChanges, key, record, loader, manageBeforeState, true, additionalData);
        recordChanges.put( key, change );
        return change;
    }

    public Iterable<RecordChange<KEY,RECORD,ADDITIONAL>> changes()
    {
        return Iterables.filter( new Predicate<RecordChange<KEY,RECORD,ADDITIONAL>>()
        {
            @Override
            public boolean accept( RecordChange<KEY, RECORD, ADDITIONAL> item )
            {
                return item.isChanged();
            }
        }, recordChanges.values() );
    }

    public static class RecordChange<KEY,RECORD,ADDITIONAL> implements RecordProxy<KEY, RECORD, ADDITIONAL>
    {
        private final Map<KEY, RecordChange<KEY, RECORD, ADDITIONAL>> allChanges;
        private final ADDITIONAL additionalData;
        private RECORD before;
        private final RECORD record;
        private final Loader<KEY,RECORD,ADDITIONAL> loader;
        private boolean changed;
        private final boolean created;
        private final KEY key;
        private final boolean manageBeforeState;

        public RecordChange(Map<KEY, RecordChange<KEY, RECORD, ADDITIONAL>> allChanges, KEY key, RECORD record,
                            Loader<KEY, RECORD, ADDITIONAL> loader, boolean manageBeforeState, boolean created, ADDITIONAL additionalData)
        {
            this.allChanges = allChanges;
            this.key = key;
            this.record = record;
            this.loader = loader;
            this.manageBeforeState = manageBeforeState;
            this.created = created;
            this.additionalData = additionalData;
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
                this.allChanges.put( key, this );
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

        @SuppressWarnings( "unchecked" )
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
