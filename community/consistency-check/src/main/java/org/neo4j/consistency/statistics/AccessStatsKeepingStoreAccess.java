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
package org.neo4j.consistency.statistics;

import org.neo4j.consistency.statistics.AccessStatistics.AccessStats;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * {@link StoreAccess} that decorates each store, feeding stats about access into {@link AccessStatistics}.
 */
public class AccessStatsKeepingStoreAccess extends StoreAccess
{
    private final AccessStatistics accessStatistics;

    public AccessStatsKeepingStoreAccess( NeoStores neoStore, AccessStatistics accessStatistics )
    {
        super( neoStore );
        this.accessStatistics = accessStatistics;
    }

    @Override
    protected <R extends AbstractBaseRecord> RecordStore<R> wrapStore( RecordStore<R> store )
    {
        AccessStats accessStats = new AccessStats( store.getClass().getSimpleName(), store.getRecordsPerPage() );
        accessStatistics.register( store, accessStats );
        return new AccessStatsKeepingRecordStore( store, accessStats );
    }

    private static class AccessStatsKeepingRecordStore<RECORD extends AbstractBaseRecord>
            extends RecordStore.Delegator<RECORD>
    {
        private final AccessStats accessStats;

        public AccessStatsKeepingRecordStore( RecordStore<RECORD> actual, AccessStats accessStats )
        {
            super( actual );
            this.accessStats = accessStats;
        }

        protected AccessStats getAccessStats()
        {
            return accessStats;
        }

        @Override
        public RECORD getRecord( long id )
        {
            accessStats.upRead( id );
            return super.getRecord( id );
        }

        @Override
        public RECORD forceGetRecord( long id )
        {
            accessStats.upRead( id );
            return super.forceGetRecord( id );
        }
    }
}
