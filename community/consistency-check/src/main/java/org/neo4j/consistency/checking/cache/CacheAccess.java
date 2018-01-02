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

import org.neo4j.consistency.checking.full.CheckStage;
import org.neo4j.consistency.statistics.Counts;
import org.neo4j.consistency.statistics.Counts.Type;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

/**
 * Just as {@link RecordAccess} is the main access point for {@link AbstractBaseRecord} and friends,
 * so is {@link CacheAccess} the main access point for cached values related to records, most often caching
 * parts of records, specific to certain {@link CheckStage stages} of the consistency check.
 *
 * The access patterns to {@link CacheAccess} is designed to have multiple threads concurrently, and so
 * {@link #client()} provides a {@link Client} that accesses the cache on the current thread's behalf.
 *
 * The cache is a compact representation of records, tied to an id, for example nodeId. There can be multiple
 * cached values per id, selected by {@code slot}.
 */
public interface CacheAccess
{
    /**
     * Client per thread for accessing cache and counts for statistics
     */
    interface Client
    {
        /**
         * Gets a cached value, put there with {@link #putToCache(long, long...)} or
         * {@link #putToCacheSingle(long, int, long)}.
         *
         * @param id the entity id this cached value is tied to.
         * @param slot which cache slot for this id.
         * @return the cached value.
         */
        long getFromCache( long id, int slot );

        /**
         * Caches all values for an id, i.e. fills all slots.
         *
         * @param id the entity id these cached values will be tied to.
         * @param cacheFields the values to cache, one per slot.
         */
        void putToCache( long id, long... cacheFields );

        /**
         * Caches a single value for an id and slot.
         *
         * @param id the entity id this cached values will be tied to.
         * @param slot the slot for the given {@code id}.
         * @param value the value to cache for this id and slot.
         */
        void putToCacheSingle( long id, int slot, long value );

        /**
         * Clears the cached values for the specified {@code id}.
         *
         * @param id the entity id to clear the cached values for.
         */
        void clearCache( long id );

        /**
         * Caches a {@link Collection} of {@link PropertyRecord} for later checking.
         *
         * @param properties property records to cache for this thread.
         */
        void putPropertiesToCache( Collection<PropertyRecord> properties );

        /**
         * Gets a cached {@link PropertyRecord} of a specific {@code id}, see {@link #putPropertiesToCache(Collection)}.
         *
         * @param id the property record id to look for.
         * @return cached {@link PropertyRecord} {@link PropertyRecord#getId() id}, or {@code null} if not found.
         */
        PropertyRecord getPropertyFromCache( long id );

        /**
         * @return cached properties.
         */
        Iterable<PropertyRecord> getPropertiesFromCache();

        /**
         * Increases the count of the specified {@code type}, for gathering statistics during a run.
         *
         * @param type counts type.
         */
        void incAndGetCount( Counts.Type type );

        /**
         * Some consistency check stages splits the id range into segments, one per thread.
         * That split is initiated by {@link CacheAccess#prepareForProcessingOfSingleStore(long)} and checker,
         * per thread, using this method.
         *
         * @param id the record id to check whether or not to process for this thread.
         * @return {@code true} if the thread represented by this client should process the record
         * of the given {@code id}, otherwise {@code false}.
         */
        boolean withinBounds( long id );
    }

    /**
     * @return {@link Client} for the current {@link Thread}.
     */
    Client client();

    /**
     * A flag for record checkers using this cache, where cached values are treated differently if
     * we're scanning through a store forwards or backwards.
     *
     * @return {@code true} if the scanning is currently set to go forward.
     */
    boolean isForward();

    /**
     * Tells this {@link CacheAccess} whether or not the current stage, i.e is scanning forwards or backwards
     * through a store or not. The cached values are treated differently depending on which. This is due to
     * the double-linked nature of some stores, specifically the relationship store.
     *
     * @param forward {@code true} if the current scanning is forwards, otherwise it's backwards.
     */
    void setForward( boolean forward );

    /**
     * Clears all cached values.
     */
    void clearCache();

    /**
     * Sets the slot sizes of the cached values.
     *
     * @param slotSize defines how many and how big the slots are for cached values that are put after this call.
     */
    void setCacheSlotSizes( int... slotSize );

    void prepareForProcessingOfSingleStore( long recordsPerCPU );

    Client EMPTY_CLIENT = new Client()
    {
        @Override
        public void putPropertiesToCache( Collection<PropertyRecord> properties )
        {
        }

        @Override
        public void putToCache( long id, long... cacheFields )
        {
        }

        @Override
        public void putToCacheSingle( long id, int slot, long value )
        {
        }

        @Override
        public void clearCache( long id )
        {
        }

        @Override
        public void incAndGetCount( Type type )
        {
        }

        @Override
        public PropertyRecord getPropertyFromCache( long id )
        {
            return null;
        }

        @Override
        public Iterable<PropertyRecord> getPropertiesFromCache()
        {
            return null;
        }

        @Override
        public long getFromCache( long id, int slot )
        {
            return 0;
        }

        @Override
        public boolean withinBounds( long id )
        {
            return false;
        }
    };

    CacheAccess EMPTY = new CacheAccess()
    {
        @Override
        public Client client()
        {
            return EMPTY_CLIENT;
        }

        @Override
        public void setForward( boolean forward )
        {
        }

        @Override
        public void setCacheSlotSizes( int... slotSizes )
        {
        }

        @Override
        public boolean isForward()
        {
            return false;
        }

        @Override
        public void clearCache()
        {
        }

        @Override
        public void prepareForProcessingOfSingleStore( long recordsPerCPU )
        {
        }
    };
}
