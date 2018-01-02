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
package org.neo4j.consistency.checking.cache;

import java.util.Collection;

import org.neo4j.consistency.checking.full.IdAssigningThreadLocal;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.consistency.statistics.Counts.Type;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

/**
 * {@link CacheAccess} that uses {@link PackedMultiFieldCache} for cache.
 */
public class DefaultCacheAccess implements CacheAccess
{
    public static final int DEFAULT_QUEUE_SIZE = 1_000;

    private final IdAssigningThreadLocal<Client> clients = new IdAssigningThreadLocal<Client>()
    {
        @Override
        protected Client initialValue( int id )
        {
            return new DefaultClient( id );
        }
    };

    private final Collection<PropertyRecord>[] propertiesProcessed;
    private boolean forwardScan = true;
    private final PackedMultiFieldCache cache;
    private long recordsPerCPU;
    private final Counts counts;

    public DefaultCacheAccess( Counts counts, int threads )
    {
        this.counts = counts;
        this.propertiesProcessed = new Collection[threads];
        this.cache = new PackedMultiFieldCache( 1, 63 );
    }

    @Override
    public Client client()
    {
        return clients.get();
    }

    @Override
    public void clearCache()
    {
        cache.clear();
    }

    @Override
    public void setCacheSlotSizes( int... slotSizes )
    {
        cache.setSlotSizes( slotSizes );
    }

    @Override
    public void setForward( boolean forward )
    {
        forwardScan = forward;
    }

    @Override
    public boolean isForward()
    {
        return forwardScan;
    }

    @Override
    public void prepareForProcessingOfSingleStore( long recordsPerCpu )
    {
        clients.resetId();
        this.recordsPerCPU = recordsPerCpu;
    }

    private class DefaultClient implements Client
    {
        private final int threadIndex;

        public DefaultClient( int threadIndex )
        {
            this.threadIndex = threadIndex;
        }

        @Override
        public long getFromCache( long id, int slot )
        {
            return cache.get( id, slot );
        }

        @Override
        public void putToCache( long id, long... values )
        {
            cache.put( id, values );
        }

        @Override
        public void putToCacheSingle( long id, int slot, long value )
        {
            cache.put( id, slot, value );
        }

        @Override
        public void clearCache( long index )
        {
            cache.clear( index );
            counts.incAndGet( Counts.Type.clearCache, threadIndex );
            counts.incAndGet( Counts.Type.activeCache, threadIndex );
        }

        @Override
        public boolean withinBounds( long id )
        {
            if ( recordsPerCPU == 0 )
            {
                // We haven't split the id space into segments per thread
                return true;
            }

            return id >= threadIndex * recordsPerCPU &&
                   id <= (threadIndex + 1) * recordsPerCPU;
        }

        @Override
        public void putPropertiesToCache( Collection<PropertyRecord> properties )
        {
            propertiesProcessed[threadIndex] = properties;
        }

        @Override
        public Iterable<PropertyRecord> getPropertiesFromCache()
        {
            return cachedProperties( true );
        }

        @Override
        public PropertyRecord getPropertyFromCache( long id )
        {
            Collection<PropertyRecord> properties = cachedProperties( false );
            if ( properties != null )
            {
                for ( PropertyRecord property : properties )
                {
                    if ( property.getId() == id )
                    {
                        return property;
                    }
                }
            }
            return null;
        }

        private Collection<PropertyRecord> cachedProperties( boolean clear )
        {
            try
            {
                return propertiesProcessed[threadIndex];
            }
            finally
            {
                if ( clear )
                {
                    propertiesProcessed[threadIndex] = null;
                }
            }
        }

        @Override
        public void incAndGetCount( Type type )
        {
            counts.incAndGet( type, threadIndex );
        }

        @Override
        public String toString()
        {
            return "Client[" + threadIndex + ", records/CPU:" + recordsPerCPU + "]";
        }
    }
}
