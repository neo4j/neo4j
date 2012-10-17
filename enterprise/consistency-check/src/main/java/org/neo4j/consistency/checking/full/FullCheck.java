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

import static org.neo4j.consistency.checking.full.MultiPassStore.ARRAYS;
import static org.neo4j.consistency.checking.full.MultiPassStore.NODES;
import static org.neo4j.consistency.checking.full.MultiPassStore.PROPERTIES;
import static org.neo4j.consistency.checking.full.MultiPassStore.RELATIONSHIPS;
import static org.neo4j.consistency.checking.full.MultiPassStore.STRINGS;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.report.ConsistencyLogger;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.MessageConsistencyLogger;
import org.neo4j.consistency.store.CacheSmallStoresRecordAccess;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.util.StringLogger;

public class FullCheck
{
    private final boolean checkPropertyOwners;
    private final TaskExecutionOrder order;
    private final ProgressMonitorFactory progressFactory;
    private final Long totalMappedMemory;

    public FullCheck( Config tuningConfiguration, ProgressMonitorFactory progressFactory )
    {
        this.checkPropertyOwners = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_property_owners );
        this.order = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_execution_order );
        this.totalMappedMemory = tuningConfiguration.get( GraphDatabaseSettings.all_stores_total_mapped_memory_size );
        this.progressFactory = progressFactory;
    }

    public ConsistencySummaryStatistics execute( StoreAccess store, StringLogger logger )
            throws ConsistencyCheckIncompleteException
    {
        ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();
        OwnerCheck ownerCheck = new OwnerCheck( checkPropertyOwners );
        execute( store, ownerCheck, recordAccess( store ), new MessageConsistencyLogger( logger ), summary );
        ownerCheck.scanForOrphanChains( progressFactory );
        if ( !summary.isConsistent() )
        {
            logger.logMessage( "Inconsistencies found: " + summary );
        }
        return summary;
    }

    void execute( StoreAccess store, CheckDecorator decorator, DiffRecordAccess recordAccess,
                  ConsistencyLogger logger, ConsistencySummaryStatistics summary )
            throws ConsistencyCheckIncompleteException
    {
        StoreProcessor processEverything = new StoreProcessor( decorator,
                new ConsistencyReporter( logger, recordAccess, summary ) );

        ProgressMonitorFactory.MultiPartBuilder progress = progressFactory.multipleParts( "Full consistency check" );
        List<StoreProcessorTask> tasks = new ArrayList<StoreProcessorTask>( 9 );

        MultiPassStore.Factory processorFactory = new MultiPassStore.Factory(
                decorator, logger, totalMappedMemory, store, recordAccess, summary );

        tasks.add( new StoreProcessorTask<NodeRecord>( store.getNodeStore(), progress,
                                                       processorFactory.createAll( PROPERTIES,
                                                                                   RELATIONSHIPS ) ) );
        tasks.add( new StoreProcessorTask<RelationshipRecord>( store.getRelationshipStore(), progress,
                                                               processorFactory.createAll( NODES,
                                                                                           PROPERTIES,
                                                                                           RELATIONSHIPS ) ) );
        tasks.add( new StoreProcessorTask<PropertyRecord>( store.getPropertyStore(), progress,
                                                           processorFactory.createAll( PROPERTIES,
                                                                                       STRINGS,
                                                                                       ARRAYS ) ) );

        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getStringStore(), progress,
                                                          processorFactory.createAll( STRINGS ) ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getArrayStore(), progress,
                                                          processorFactory.createAll( ARRAYS ) ) );

        tasks.add( new StoreProcessorTask<RelationshipTypeRecord>( store.getRelationshipTypeStore(), progress,
                                                                   processEverything ) );
        tasks.add( new StoreProcessorTask<PropertyIndexRecord>( store.getPropertyIndexStore(), progress,
                                                                processEverything ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getTypeNameStore(), progress, processEverything ) );
        tasks.add( new StoreProcessorTask<DynamicRecord>( store.getPropertyKeyStore(), progress, processEverything ) );

        order.execute( processEverything, tasks, progress.build() );
    }

    static DiffRecordAccess recordAccess( StoreAccess store )
    {
        return new CacheSmallStoresRecordAccess(
                new DirectRecordAccess( store ),
                readAllRecords( PropertyIndexRecord.class, store.getPropertyIndexStore() ),
                readAllRecords( RelationshipTypeRecord.class, store.getRelationshipTypeStore() ) );
    }

    private static <T extends AbstractBaseRecord> T[] readAllRecords( Class<T> type, RecordStore<T> store )
    {
        @SuppressWarnings("unchecked")
        T[] records = (T[]) Array.newInstance( type, (int) store.getHighId() );
        for ( int i = 0; i < records.length; i++ )
        {
            records[i] = store.forceGetRecord( i );
        }
        return records;
    }

}
