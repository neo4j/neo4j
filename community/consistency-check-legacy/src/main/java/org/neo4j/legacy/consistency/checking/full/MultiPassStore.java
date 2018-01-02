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
package org.neo4j.legacy.consistency.checking.full;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.legacy.consistency.checking.CheckDecorator;
import org.neo4j.legacy.consistency.report.ConsistencyReporter;
import org.neo4j.legacy.consistency.report.InconsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.FilteringRecordAccess;

public enum MultiPassStore
{
    NODES
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }
            },
    RELATIONSHIPS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getRelationshipStore();
                }
            },
    PROPERTIES
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getPropertyStore();
                }
            },
    PROPERTY_KEYS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getPropertyKeyTokenStore();
                }
            },
    STRINGS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }
            },
    ARRAYS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }
            },
    LABELS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getLabelTokenStore();
                }
            },
    RELATIONSHIP_GROUPS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getRelationshipGroupStore();
                }
            };

    public static boolean recordInCurrentPass( long id, int iPass, long recordsPerPass )
    {
        return id >= iPass * recordsPerPass && id < (iPass + 1) * recordsPerPass;
    }

    public List<DiffRecordAccess> multiPassFilters( long memoryPerPass, StoreAccess storeAccess,
            DiffRecordAccess recordAccess, MultiPassStore[] stores )
    {
        List<DiffRecordAccess> filteringStores = new ArrayList<>();
        RecordStore<?> recordStore = getRecordStore( storeAccess );
        long recordsPerPass = memoryPerPass / recordStore.getRecordSize();
        long highId = recordStore.getHighId();
        for ( int iPass = 0; iPass * recordsPerPass <= highId; iPass++ )
        {
            filteringStores.add( new FilteringRecordAccess( recordAccess, iPass, recordsPerPass, this, stores ) );
        }
        return filteringStores;
    }


    abstract RecordStore<?> getRecordStore( StoreAccess storeAccess );

    static class Factory
    {
        private final CheckDecorator decorator;
        private final DiffRecordAccess recordAccess;
        private final long totalMappedMemory;
        private final StoreAccess storeAccess;
        private final InconsistencyReport report;

        Factory( CheckDecorator decorator, long totalMappedMemory,
                 StoreAccess storeAccess, DiffRecordAccess recordAccess, InconsistencyReport report )
        {
            this.decorator = decorator;
            this.totalMappedMemory = totalMappedMemory;
            this.storeAccess = storeAccess;
            this.recordAccess = recordAccess;
            this.report = report;
        }

        ConsistencyReporter[] reporters( TaskExecutionOrder order, MultiPassStore... stores )
        {
            if ( order == TaskExecutionOrder.MULTI_PASS )
            {
                return reporters( stores );
            }
            else
            {
                return new ConsistencyReporter[]{new ConsistencyReporter( recordAccess, report )};
            }
        }

        ConsistencyReporter[] reporters( MultiPassStore... stores )
        {
            List<ConsistencyReporter> result = new ArrayList<>();
            for ( MultiPassStore store : stores )
            {
                List<DiffRecordAccess> filters = store.multiPassFilters( totalMappedMemory, storeAccess,
                        recordAccess, stores );
                for ( DiffRecordAccess filter : filters )
                {
                    result.add( new ConsistencyReporter( filter, report ) );
                }
            }
            return result.toArray( new ConsistencyReporter[result.size()] );
        }

        StoreProcessor[] processors( MultiPassStore... stores )
        {
            List<StoreProcessor> result = new ArrayList<>();
            for ( ConsistencyReporter reporter : reporters( stores ) )
            {
                result.add( new StoreProcessor( decorator, reporter ) );
            }
            return result.toArray( new StoreProcessor[result.size()] );
        }
    }
}
