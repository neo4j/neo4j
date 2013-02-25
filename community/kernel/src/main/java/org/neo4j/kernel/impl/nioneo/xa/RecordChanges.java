/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

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
public class RecordChanges<KEY,RECORD extends AbstractBaseRecord,ADDITIONAL>
{
    private final Map<KEY, RecordChange<KEY,RECORD,ADDITIONAL>> recordChanges =
            new HashMap<KEY,RecordChange<KEY,RECORD,ADDITIONAL>>();
    private final Loader<KEY,RECORD,ADDITIONAL> loader;
    private final boolean manageBeforeState;

    public RecordChanges( Loader<KEY,RECORD,ADDITIONAL> loader, boolean manageBeforeState )
    {
        this.loader = loader;
        this.manageBeforeState = manageBeforeState;
    }
    
    public RecordChange<KEY,RECORD,ADDITIONAL> getIfLoaded( KEY key )
    {
        return recordChanges.get( key );
    }
    
    public RecordChange<KEY,RECORD,ADDITIONAL> getOrLoad( KEY key, ADDITIONAL additionalData )
    {
        RecordChange<KEY,RECORD,ADDITIONAL> result = recordChanges.get( key );
        if ( result == null )
        {
            result = new RecordChange<KEY,RECORD,ADDITIONAL>( recordChanges, key,
                    loader.load( key, additionalData ), loader, manageBeforeState, false );
        }
        return result;
    }
    
    public int changeSize()
    {
        // TODO optimize?
        return count( changes() );
    }
    
    public void clear()
    {
        recordChanges.clear();
    }
    
    public RecordChange<KEY,RECORD,ADDITIONAL> create( KEY key, ADDITIONAL additionalData )
    {
        if ( recordChanges.containsKey( key ) )
            throw new IllegalStateException( key + " already exists" );
        
        RECORD record = loader.newUnused( key, additionalData );
        RecordChange<KEY, RECORD, ADDITIONAL> change = new RecordChange<KEY,RECORD,ADDITIONAL>(
                recordChanges, key, record, loader, manageBeforeState, true );
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
    
    public static class RecordChange<KEY,RECORD extends AbstractBaseRecord,ADDITIONAL>
    {
        private final Map<KEY, RecordChange<KEY, RECORD, ADDITIONAL>> allChanges;
        private RECORD before;
        private final RECORD record;
        private final Loader<KEY,RECORD,ADDITIONAL> loader;
        private boolean changed;
        private final boolean created;
        private final KEY key;
        private final boolean manageBeforeState;

        public RecordChange( Map<KEY, RecordChange<KEY,RECORD,ADDITIONAL>> allChanges, KEY key, RECORD record,
                Loader<KEY,RECORD,ADDITIONAL> loader, boolean manageBeforeState, boolean created )
        {
            this.allChanges = allChanges;
            this.key = key;
            this.record = record;
            this.loader = loader;
            this.manageBeforeState = manageBeforeState;
            this.created = created;
        }
        
        KEY getKey()
        {
            return key;
        }
        
        RECORD forChangingLinkage()
        {
            return prepareForChange();
        }

        RECORD forChangingData()
        {
            makeHeavy();
            return prepareForChange();
        }
        
        private RECORD prepareForChange()
        {
            ensureBeforeInstantiated();
            if ( !this.changed )
            {
                this.allChanges.put( key, this );
                this.changed = true;
            }
            return this.record;
        }
        
        private void makeHeavy()
        {
            if ( !created )
            {
                loader.makeHeavy( record );
                if ( before != null )
                    loader.makeHeavy( before );
            }
        }
        
        RECORD forReadingLinkage()
        {
            return this.record;
        }
        
        RECORD forReadingData()
        {
            makeHeavy();
            return this.record;
        }
        
        public boolean isChanged()
        {
            return this.changed;
        }
        
        public RECORD getBefore()
        {
            ensureBeforeInstantiated();
            if ( !manageBeforeState )
                throw new UnsupportedOperationException( "This RecordChanges instance doesn't manage before-state" );
            return before;
        }

        @SuppressWarnings( "unchecked" )
        private void ensureBeforeInstantiated()
        {
            if ( manageBeforeState && this.before == null )
            {
                this.before = (RECORD) record.clone();
            }
        }

        public boolean isCreated()
        {
            return created;
        }
    }
    
    public interface Loader<KEY,RECORD extends AbstractBaseRecord,ADDITIONAL>
    {
        RECORD newUnused( KEY key, ADDITIONAL additionalData );
        
        RECORD load( KEY key, ADDITIONAL additionalData );
        
        void makeHeavy( RECORD record );
    }
}
