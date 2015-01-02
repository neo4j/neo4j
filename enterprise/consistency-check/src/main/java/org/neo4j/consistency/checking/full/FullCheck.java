/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.lang.reflect.Array;
import java.util.List;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.store.CacheSmallStoresRecordAccess;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.util.StringLogger;

public class FullCheck
{
    private final boolean checkPropertyOwners;
    private final boolean checkLabelScanStore;
    private final boolean checkIndexes;
    private final TaskExecutionOrder order;
    private final ProgressMonitorFactory progressFactory;
    private final Long totalMappedMemory;

    public FullCheck( Config tuningConfiguration, ProgressMonitorFactory progressFactory )
    {
        this.checkPropertyOwners = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_property_owners );
        this.checkLabelScanStore = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_label_scan_store );
        this.checkIndexes = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_indexes );
        this.order = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_execution_order );
        this.totalMappedMemory = tuningConfiguration.get( GraphDatabaseSettings.all_stores_total_mapped_memory_size );
        this.progressFactory = progressFactory;
    }

    public ConsistencySummaryStatistics execute( DirectStoreAccess stores, StringLogger logger )
            throws ConsistencyCheckIncompleteException
    {
        ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();
        InconsistencyReport report = new InconsistencyReport( new InconsistencyMessageLogger( logger ), summary );

        OwnerCheck ownerCheck = new OwnerCheck( checkPropertyOwners );
        execute( stores, ownerCheck, recordAccess( stores.nativeStores() ), report );
        ownerCheck.scanForOrphanChains( progressFactory );

        if ( !summary.isConsistent() )
        {
            logger.logMessage( "Inconsistencies found: " + summary );
        }
        return summary;
    }

    void execute( final DirectStoreAccess directStoreAccess, CheckDecorator decorator, final DiffRecordAccess recordAccess,
                  final InconsistencyReport report )
            throws ConsistencyCheckIncompleteException
    {
        final ConsistencyReporter reporter = new ConsistencyReporter( recordAccess, report );
        StoreProcessor processEverything = new StoreProcessor( decorator, reporter );

        ProgressMonitorFactory.MultiPartBuilder progress = progressFactory.multipleParts( "Full consistency check" );

        final StoreAccess nativeStores = directStoreAccess.nativeStores();
        try ( IndexAccessors indexes = new IndexAccessors( directStoreAccess.indexes(), nativeStores.getSchemaStore() ) )
        {
            MultiPassStore.Factory multiPass = new MultiPassStore.Factory(
                    decorator, totalMappedMemory, nativeStores, recordAccess, report );
            List<StoppableRunnable> tasks = new ConsistencyCheckTasks( progress, order, processEverything ).createTasks(
                    nativeStores,
                    directStoreAccess.labelScanStore(),
                    indexes,
                    multiPass,
                    reporter,
                    checkLabelScanStore,
                    checkIndexes
            );

            order.execute( tasks, progress.build() );
        }
        catch ( Exception e )
        {
            throw new ConsistencyCheckIncompleteException( e );
        }

    }

    static DiffRecordAccess recordAccess( StoreAccess store )
    {
        return new CacheSmallStoresRecordAccess(
                new DirectRecordAccess( store ),
                readAllRecords( PropertyKeyTokenRecord.class, store.getPropertyKeyTokenStore() ),
                readAllRecords( RelationshipTypeTokenRecord.class, store.getRelationshipTypeTokenStore() ),
                readAllRecords( LabelTokenRecord.class, store.getLabelTokenStore() ) );
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
