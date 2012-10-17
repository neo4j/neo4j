/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import static org.neo4j.consistency.store.RecordReference.SkippingReference.skipReference;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.report.ConsistencyLogger;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;

enum MultiPassStore
{
    NODES()
            {
                @Override
                RecordStore getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }

                @Override
                DiffRecordAccess filter( final DiffRecordAccess recordAccess, final int iPass,
                                         final long recordsPerPass )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<NodeRecord> node( long id )
                        {
                            if ( recordInCurrentPass( id, iPass, recordsPerPass ) )
                            {
                                return recordAccess.node( id );
                            }
                            return skipReference();
                        }
                    };
                }
            },
    RELATIONSHIPS()
            {
                @Override
                RecordStore getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getRelationshipStore();
                }

                @Override
                DiffRecordAccess filter( final DiffRecordAccess recordAccess, final int iPass,
                                         final long recordsPerPass )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<RelationshipRecord> relationship( long id )
                        {
                            if ( recordInCurrentPass( id, iPass, recordsPerPass ) )
                            {
                                return recordAccess.relationship( id );
                            }
                            return skipReference();
                        }
                    };
                }
            },
    PROPERTIES()
            {
                @Override
                RecordStore getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getPropertyStore();
                }

                @Override
                DiffRecordAccess filter( final DiffRecordAccess recordAccess, final int iPass,
                                         final long recordsPerPass )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<PropertyRecord> property( long id )
                        {
                            if ( recordInCurrentPass( id, iPass, recordsPerPass ) )
                            {
                                return recordAccess.property( id );
                            }
                            return skipReference();
                        }
                    };
                }
            },
    STRINGS()
            {
                @Override
                RecordStore getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }

                @Override
                DiffRecordAccess filter( final DiffRecordAccess recordAccess, final int iPass,
                                         final long recordsPerPass )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<DynamicRecord> string( long id )
                        {
                            if ( recordInCurrentPass( id, iPass, recordsPerPass ) )
                            {
                                return recordAccess.string( id );
                            }
                            return skipReference();
                        }
                    };
                }
            },
    ARRAYS()
            {
                @Override
                RecordStore getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }

                @Override
                DiffRecordAccess filter( final DiffRecordAccess recordAccess, final int iPass,
                                         final long recordsPerPass )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<DynamicRecord> array( long id )
                        {
                            if ( recordInCurrentPass( id, iPass, recordsPerPass ) )
                            {
                                return recordAccess.array( id );
                            }
                            return skipReference();
                        }
                    };
                }
            };

    private static boolean recordInCurrentPass( long id, int iPass, long recordsPerPass )
    {
        return id >= iPass * recordsPerPass && id < (iPass + 1) * recordsPerPass;
    }

    public List<DiffRecordAccess> multiPassFilters( long memoryPerPass, StoreAccess storeAccess,
                                                    DiffRecordAccess recordAccess )
    {
        ArrayList<DiffRecordAccess> filters = new ArrayList<DiffRecordAccess>();
        RecordStore recordStore = getRecordStore( storeAccess );
        long recordsPerPass = memoryPerPass / recordStore.getRecordSize();
        long highId = recordStore.getHighId();
        for ( int iPass = 0; iPass * recordsPerPass <= highId; iPass++ )
        {
            filters.add( filter( recordAccess, iPass, recordsPerPass ) );
        }
        return filters;
    }

    abstract RecordStore getRecordStore( StoreAccess storeAccess );

    abstract DiffRecordAccess filter( DiffRecordAccess recordAccess, int iPass, long recordsPerPass );

    static class Factory
    {
        private final CheckDecorator decorator;
        private final ConsistencyLogger logger;
        private final DiffRecordAccess recordAccess;
        private final ConsistencySummaryStatistics summary;
        private final long totalMappedMemory;
        private final StoreAccess storeAccess;

        Factory( CheckDecorator decorator, ConsistencyLogger logger, long totalMappedMemory,
                 StoreAccess storeAccess, DiffRecordAccess recordAccess, ConsistencySummaryStatistics summary )
        {
            this.decorator = decorator;
            this.logger = logger;
            this.totalMappedMemory = totalMappedMemory;
            this.storeAccess = storeAccess;
            this.recordAccess = recordAccess;
            this.summary = summary;
        }

        StoreProcessor[] createAll( MultiPassStore... stores )
        {
            List<StoreProcessor> result = new ArrayList<StoreProcessor>();
            for ( MultiPassStore store : stores )
            {
                List<DiffRecordAccess> filters = store.multiPassFilters( totalMappedMemory, storeAccess, recordAccess );
                for ( DiffRecordAccess filter : filters )
                {
                    result.add( new StoreProcessor( decorator, new ConsistencyReporter( logger, filter, summary ) ) );
                }
            }
            return result.toArray( new StoreProcessor[result.size()] );
        }
    }
}
