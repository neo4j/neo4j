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
package org.neo4j.unsafe.batchinsert;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractRecordStore;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccess;
import org.neo4j.kernel.impl.util.FlyweightPool;

/**
 * Provides direct access to records in a store. Changes are batched up and written whenever {@link #commit()}
 * is called, or {@link #close()} for that matter.
 */
public class DirectRecordAccess<KEY extends Comparable<KEY>,RECORD extends AbstractBaseRecord,ADDITIONAL>
        implements RecordAccess<KEY,RECORD,ADDITIONAL>
{
    private final AbstractRecordStore<RECORD> store;
    private final Loader<KEY, RECORD, ADDITIONAL> loader;
    private final SortedMap<KEY,DirectRecordProxy> batch = new TreeMap<>( new Comparator<KEY>()
    {
        @Override
        public int compare( KEY o1, KEY o2 )
        {
            return -o1.compareTo( o2 );
        }
    });
    private boolean changed;
    private final FlyweightPool<DirectRecordProxy> proxyFlyweightPool;

    public DirectRecordAccess( AbstractRecordStore<RECORD> store, Loader<KEY, RECORD, ADDITIONAL> loader )
    {
        this.store = store;
        this.loader = loader;
        proxyFlyweightPool = new FlyweightPool<DirectRecordProxy>( 100 )
        {
            @Override
            protected DirectRecordProxy create()
            {
                return new DirectRecordProxy();
            }
        };
    }

    @Override
    public RecordProxy<KEY, RECORD, ADDITIONAL> getOrLoad( KEY key, ADDITIONAL additionalData )
    {
        DirectRecordProxy loaded = batch.get( key );
        if ( loaded != null )
        {
            return loaded;
        }
        return putInBatch( key, proxy( key, loader.load( key, additionalData ), additionalData ) );
    }

    private RecordProxy<KEY, RECORD, ADDITIONAL> putInBatch( KEY key, DirectRecordProxy proxy )
    {
        DirectRecordProxy previous = batch.put( key, proxy );
        assert previous == null;
        return proxy;
    }

    @Override
    public RecordProxy<KEY, RECORD, ADDITIONAL> create( KEY key, ADDITIONAL additionalData )
    {
        return putInBatch( key, proxy( key, loader.newUnused( key, additionalData ), additionalData ) );
    }

    private DirectRecordProxy proxy( final KEY key, final RECORD record, final ADDITIONAL additionalData )
    {
        DirectRecordProxy result = proxyFlyweightPool.acquire();
        result.bind( key, record, additionalData );
        return result;
    }

    private class DirectRecordProxy implements RecordProxy<KEY,RECORD,ADDITIONAL>
    {
        private KEY key;
        private RECORD record;
        private ADDITIONAL additionalData;
        private boolean changed;

        public void bind( KEY key, RECORD record, ADDITIONAL additionalData )
        {
            this.key = key;
            this.record = record;
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
            prepareChange();
            return record;
        }

        private void prepareChange()
        {
            changed = true;
            DirectRecordAccess.this.changed = true;
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
    }

    @Override
    public void close()
    {
        commit();
    }

    public void commit()
    {
        if ( !changed )
        {
            return;
        }

        for ( DirectRecordProxy proxy : batch.values() )
        {
            proxy.store();
            proxyFlyweightPool.release( proxy );
        }
        batch.clear();
    }
}
