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
package org.neo4j.unsafe.impl.batchimport.store;

import java.util.Collection;

import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.util.collection.ArrayCollection;

/**
 * {@link RecordAccess} optimized for batching and an access pattern where records are created sequentially.
 * Mostly here as a bridge between a batch importer and existing record logic in {@link NeoStoreTransactionContext}
 * and friends.
 */
public abstract class BatchingRecordAccess<KEY,RECORD,ADDITIONAL> implements RecordAccess<KEY,RECORD,ADDITIONAL>
{
    private final Collection<RecordProxy<KEY,RECORD,ADDITIONAL>> proxies = new ArrayCollection<>( 1000 );

    @Override
    public RecordProxy<KEY,RECORD,ADDITIONAL> getOrLoad( KEY key, ADDITIONAL additionalData )
    {
        throw new UnsupportedOperationException( "We only support creations here" );
    }

    @Override
    public RecordProxy<KEY,RECORD,ADDITIONAL> create( KEY key, ADDITIONAL additionalData )
    {
        RECORD record = createRecord( key, additionalData );
        BatchingRecordProxy<KEY,RECORD,ADDITIONAL> proxy = new BatchingRecordProxy<>( key, record, additionalData );
        proxies.add( proxy );
        return proxy;
    }

    protected abstract RECORD createRecord( KEY key, ADDITIONAL additionalData );

    public Iterable<RECORD> records()
    {
        return new IterableWrapper<RECORD,RecordProxy<KEY,RECORD,ADDITIONAL>>( proxies )
        {
            @Override
            protected RECORD underlyingObjectToObject( RecordProxy<KEY,RECORD,ADDITIONAL> object )
            {
                return object.forReadingLinkage();
            }
        };
    }

    @Override
    public RecordProxy<KEY,RECORD,ADDITIONAL> getIfLoaded( KEY key )
    {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public void setTo( KEY key, RECORD newRecord, ADDITIONAL additionalData )
    {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public int changeSize()
    {
        return proxies.size();
    }

    @Override
    public Iterable<RecordProxy<KEY,RECORD,ADDITIONAL>> changes()
    {
        return proxies;
    }

    @Override
    public void close()
    {   // Fast clearing due to ArrayCollection
        proxies.clear();
    }

    public static class BatchingRecordProxy<KEY,RECORD,ADDITIONAL> implements RecordProxy<KEY,RECORD,ADDITIONAL>
    {
        private final KEY key;
        private final RECORD record;
        private final ADDITIONAL additional;
        private boolean changed;

        private BatchingRecordProxy( KEY key, RECORD record, ADDITIONAL additional )
        {
            this.key = key;
            this.record = record;
            this.additional = additional;
        }

        @Override
        public KEY getKey()
        {
            return key;
        }

        @Override
        public RECORD forChangingLinkage()
        {
            return record;
        }

        @Override
        public RECORD forChangingData()
        {
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
            return record;
        }

        @Override
        public ADDITIONAL getAdditionalData()
        {
            return additional;
        }

        @Override
        public RECORD getBefore()
        {
            return null;
        }

        @Override
        public boolean isChanged()
        {
            return true;
        }

        @Override
        public boolean isCreated()
        {
            return true;
        }
    }
}
