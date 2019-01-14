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
package org.neo4j.unsafe.batchinsert.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.util.statistics.IntCounter;

/**
 * Provides direct access to records in a store. Changes are batched up and written whenever {@link #commit()}
 * is called, or {@link #close()} for that matter.
 */
public class DirectRecordAccess<RECORD extends AbstractBaseRecord,ADDITIONAL>
        implements RecordAccess<RECORD,ADDITIONAL>
{
    private final RecordStore<RECORD> store;
    private final Loader<RECORD, ADDITIONAL> loader;
    private final Map<Long,DirectRecordProxy> batch = new HashMap<>();

    private final IntCounter changeCounter = new IntCounter();

    public DirectRecordAccess( RecordStore<RECORD> store, Loader<RECORD, ADDITIONAL> loader )
    {
        this.store = store;
        this.loader = loader;
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getOrLoad( long key, ADDITIONAL additionalData )
    {
        DirectRecordProxy loaded = batch.get( key );
        if ( loaded != null )
        {
            return loaded;
        }
        return proxy( key, loader.load( key, additionalData ), additionalData, false );
    }

    private RecordProxy<RECORD, ADDITIONAL> putInBatch( long key, DirectRecordProxy proxy )
    {
        DirectRecordProxy previous = batch.put( key, proxy );
        assert previous == null;
        return proxy;
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> create( long key, ADDITIONAL additionalData )
    {
        return proxy( key, loader.newUnused( key, additionalData ), additionalData, true );
    }

    @Override
    public RecordProxy<RECORD,ADDITIONAL> getIfLoaded( long key )
    {
        return batch.get( key );
    }

    @Override
    public void setTo( long key, RECORD newRecord, ADDITIONAL additionalData )
    {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public RecordProxy<RECORD,ADDITIONAL> setRecord( long key, RECORD record, ADDITIONAL additionalData )
    {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public int changeSize()
    {
        return changeCounter.value();
    }

    @Override
    public Iterable<RecordProxy<RECORD,ADDITIONAL>> changes()
    {
        return new IterableWrapper<RecordProxy<RECORD,ADDITIONAL>,DirectRecordProxy>(
                batch.values() )
        {
            @Override
            protected RecordProxy<RECORD,ADDITIONAL> underlyingObjectToObject( DirectRecordProxy object )
            {
                return object;
            }
        };
    }

    private DirectRecordProxy proxy( final long key, final RECORD record, final ADDITIONAL additionalData, boolean created )
    {
        return new DirectRecordProxy( key, record, additionalData, created );
    }

    private class DirectRecordProxy implements RecordProxy<RECORD,ADDITIONAL>
    {
        private final long key;
        private final RECORD record;
        private final ADDITIONAL additionalData;
        private boolean changed;
        private final boolean created;

        DirectRecordProxy( long key, RECORD record, ADDITIONAL additionalData, boolean created )
        {
            this.key = key;
            this.record = record;
            this.additionalData = additionalData;
            if ( created )
            {
                prepareChange();
            }
            this.created = created;
        }

        @Override
        public long getKey()
        {
            return key;
        }

        @Override
        public RECORD forChangingLinkage()
        {
            prepareChange();
            return record;
        }

        private void prepareChange()
        {
            if ( !changed )
            {
                changed = true;
                putInBatch( key, this );
                changeCounter.increment();
            }
        }

        @Override
        public RECORD forChangingData()
        {
            loader.ensureHeavy( record );
            prepareChange();
            return record;
        }

        @Override
        public RECORD forReadingLinkage()
        {
            return record;
        }

        @Override
        public RECORD forReadingData()
        {
            loader.ensureHeavy( record );
            return record;
        }

        @Override
        public ADDITIONAL getAdditionalData()
        {
            return additionalData;
        }

        @Override
        public RECORD getBefore()
        {
            return loader.load( key, additionalData );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        public void store()
        {
            if ( changed )
            {
                store.updateRecord( record );
            }
        }

        @Override
        public boolean isChanged()
        {
            return changed;
        }

        @Override
        public boolean isCreated()
        {
            return created;
        }
    }

    @Override
    public void close()
    {
        commit();
    }

    public void commit()
    {
        if ( changeCounter.value() == 0 )
        {
            return;
        }

        List<DirectRecordProxy> directRecordProxies = new ArrayList<>( batch.values() );
        directRecordProxies.sort( ( o1, o2 ) -> Long.compare( -o1.getKey(), o2.getKey() ) );
        for ( DirectRecordProxy proxy : directRecordProxies )
        {
            proxy.store();
        }
        changeCounter.clear();
        batch.clear();
    }
}
