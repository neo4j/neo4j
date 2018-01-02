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
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.NodeRecordCheck;
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencyReporter.Monitor;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.store.FilteringRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
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

    public List<RecordAccess> multiPassFilters( RecordAccess recordAccess, MultiPassStore[] stores )
    {
        List<RecordAccess> filteringStores = new ArrayList<>();
        filteringStores.add( new FilteringRecordAccess( recordAccess, this, stores ) );
        return filteringStores;
    }

    public RecordAccess multiPassFilter( RecordAccess recordAccess, MultiPassStore... stores )
    {
        return new FilteringRecordAccess( recordAccess, this, stores );
    }

    abstract RecordStore<?> getRecordStore( StoreAccess storeAccess );

    static class Factory
    {
        private final CheckDecorator decorator;
        private final RecordAccess recordAccess;
        private final InconsistencyReport report;
        private final CacheAccess cacheAccess;
        private final Monitor monitor;

        Factory( CheckDecorator decorator,
                RecordAccess recordAccess, CacheAccess cacheAccess, InconsistencyReport report, Monitor monitor )
        {
            this.decorator = decorator;
            this.recordAccess = recordAccess;
            this.cacheAccess = cacheAccess;
            this.report = report;
            this.monitor = monitor;
        }

        ConsistencyReporter[] reporters( MultiPassStore... stores )
        {
            List<ConsistencyReporter> result = new ArrayList<>();
            for ( MultiPassStore store : stores )
            {
                List<RecordAccess> filters = store.multiPassFilters( recordAccess, stores );
                for ( RecordAccess filter : filters )
                {
                    result.add( new ConsistencyReporter( filter, report ) );
                }
            }
            return result.toArray( new ConsistencyReporter[result.size()] );
        }

        ConsistencyReporter reporter( MultiPassStore store )
        {
            RecordAccess filter = store.multiPassFilter( recordAccess, store );
            return new ConsistencyReporter( filter, report, monitor ) ;
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
