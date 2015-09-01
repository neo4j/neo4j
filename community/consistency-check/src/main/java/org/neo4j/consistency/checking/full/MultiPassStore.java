/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.NodeRecordCheck;
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.FilteringRecordAccess;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;

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

    public List<DiffRecordAccess> multiPassFilters( DiffRecordAccess recordAccess, MultiPassStore[] stores )
    {
        List<DiffRecordAccess> filteringStores = new ArrayList<>();
        filteringStores.add( new FilteringRecordAccess( recordAccess, this, stores ) );
        return filteringStores;
    }

    public DiffRecordAccess multiPassFilter( DiffRecordAccess recordAccess, MultiPassStore... stores )
    {
        return new FilteringRecordAccess( recordAccess, this, stores );
    }

    abstract RecordStore<?> getRecordStore( StoreAccess storeAccess );

    static class Factory
    {
        private final CheckDecorator decorator;
        private final DiffRecordAccess recordAccess;
        private final InconsistencyReport report;
        private final CacheAccess cacheAccess;

        Factory( CheckDecorator decorator,
                DiffRecordAccess recordAccess, CacheAccess cacheAccess, InconsistencyReport report )
        {
            this.decorator = decorator;
            this.recordAccess = recordAccess;
            this.cacheAccess = cacheAccess;
            this.report = report;
        }

        ConsistencyReporter[] reporters( MultiPassStore... stores )
        {
            List<ConsistencyReporter> result = new ArrayList<>();
            for ( MultiPassStore store : stores )
            {
                List<DiffRecordAccess> filters = store.multiPassFilters( recordAccess, stores );
                for ( DiffRecordAccess filter : filters )
                {
                    result.add( new ConsistencyReporter( filter, report ) );
                }
            }
            return result.toArray( new ConsistencyReporter[result.size()] );
        }

        StoreProcessor[] processors( Stage stage, MultiPassStore... stores )
        {
            List<StoreProcessor> result = new ArrayList<>();
            for ( ConsistencyReporter reporter : reporters( stores ) )
            {
                result.add( new StoreProcessor( decorator, reporter, stage, cacheAccess ) );
            }
            return result.toArray( new StoreProcessor[result.size()] );
        }

        ConsistencyReporter reporter( MultiPassStore store )
        {
            DiffRecordAccess filter = store.multiPassFilter( recordAccess, store );
            return new ConsistencyReporter( filter, report ) ;
        }

        StoreProcessor processor( Stage stage, MultiPassStore store )
        {
            return new StoreProcessor( decorator, reporter(store), stage, cacheAccess );
        }

        public void reDecorateNode( StoreProcessor processer, NodeRecordCheck newChecker, boolean sparseNode )
        {
            processer.reDecorateNode(decorator, newChecker, sparseNode);
        }

        public void reDecorateRelationship( StoreProcessor processer, RelationshipRecordCheck newChecker )
        {
            processer.reDecorateRelationship(decorator, newChecker );
        }
    }
}
