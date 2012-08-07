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

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.report.ConsistencyLogger;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.consistency.store.SkippingRecordAccess;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

enum FilteringStoreProcessor
{
    EVERYTHING
            {
                @Override
                DiffRecordAccess filter( DiffRecordAccess recordAccess )
                {
                    return recordAccess;
                }
            },
    PROPERTIES_ONLY
            {
                @Override
                DiffRecordAccess filter( DiffRecordAccess recordAccess )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<PropertyRecord> property( long id )
                        {
                            return recordAccess.property( id );
                        }
                    };
                }
            },
    RELATIONSHIPS_ONLY
            {
                @Override
                DiffRecordAccess filter( DiffRecordAccess recordAccess )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<RelationshipRecord> relationship( long id )
                        {
                            return recordAccess.relationship( id );
                        }
                    };
                }
            },
    NODES_ONLY
            {
                @Override
                DiffRecordAccess filter( DiffRecordAccess recordAccess )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<NodeRecord> node( long id )
                        {
                            return recordAccess.node( id );
                        }
                    };
                }
            },
    STRINGS_ONLY
            {
                @Override
                DiffRecordAccess filter( DiffRecordAccess recordAccess )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<DynamicRecord> string( long id )
                        {
                            return recordAccess.string( id );
                        }
                    };
                }
            },
    ARRAYS_ONLY
            {
                @Override
                DiffRecordAccess filter( DiffRecordAccess recordAccess )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<DynamicRecord> array( long id )
                        {
                            return recordAccess.array( id );
                        }
                    };
                }
            },
    DYNAMIC_PROPERTIES_ONLY
            {
                @Override
                DiffRecordAccess filter( DiffRecordAccess recordAccess )
                {
                    return new SkipAllButCached( recordAccess )
                    {
                        @Override
                        public RecordReference<DynamicRecord> string( long id )
                        {
                            return recordAccess.string( id );
                        }

                        @Override
                        public RecordReference<DynamicRecord> array( long id )
                        {
                            return recordAccess.array( id );
                        }
                    };
                }
            };

    static class Factory
    {
        private final CheckDecorator decorator;
        private final ConsistencyLogger logger;
        private final DiffRecordAccess recordAccess;
        private final ConsistencySummaryStatistics summary;

        Factory( CheckDecorator decorator, ConsistencyLogger logger,
                 DiffRecordAccess recordAccess, ConsistencySummaryStatistics summary )
        {

            this.decorator = decorator;
            this.logger = logger;
            this.recordAccess = recordAccess;
            this.summary = summary;
        }

        StoreProcessor create( FilteringStoreProcessor type )
        {
            return new StoreProcessor( decorator,
                                       new ConsistencyReporter( logger, type.filter( recordAccess ), summary ) );
        }

        StoreProcessor[] createAll( FilteringStoreProcessor... types )
        {
            StoreProcessor[] result = new StoreProcessor[types.length];
            for ( int i = 0; i < result.length; i++ )
            {
                result[i] = create( types[i] );
            }
            return result;
        }
    }

    abstract DiffRecordAccess filter( DiffRecordAccess recordAccess );

    private static class SkipAllButCached extends SkippingRecordAccess
    {
        final DiffRecordAccess recordAccess;

        private SkipAllButCached( DiffRecordAccess recordAccess )
        {
            this.recordAccess = recordAccess;
        }

        @Override
        public RecordReference<PropertyIndexRecord> propertyKey( int id )
        {
            return recordAccess.propertyKey( id );
        }

        @Override
        public RecordReference<RelationshipTypeRecord> relationshipLabel( int id )
        {
            return recordAccess.relationshipLabel( id );
        }
    }
}
