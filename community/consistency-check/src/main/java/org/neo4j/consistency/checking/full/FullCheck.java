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

import java.lang.reflect.Array;
import java.util.List;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencyReporter.Monitor;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.consistency.store.CacheSmallStoresRecordAccess;
import org.neo4j.consistency.store.DirectRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.Log;

import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;

public class FullCheck
{
    private final boolean checkPropertyOwners;
    private final boolean checkLabelScanStore;
    private final boolean checkIndexes;
    private final ProgressMonitorFactory progressFactory;
    private final IndexSamplingConfig samplingConfig;
    private final boolean checkGraph;
    private final int threads;
    private final Statistics statistics;

    public FullCheck( Config tuningConfiguration, ProgressMonitorFactory progressFactory,
            Statistics statistics, int threads )
    {
        this.statistics = statistics;
        this.threads = threads;
        this.checkPropertyOwners = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_property_owners );
        this.checkLabelScanStore = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_label_scan_store );
        this.checkIndexes = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_indexes );
        this.checkGraph = tuningConfiguration.get( ConsistencyCheckSettings.consistency_check_graph );
        this.samplingConfig = new IndexSamplingConfig( tuningConfiguration );
        this.progressFactory = progressFactory;
    }

    public ConsistencySummaryStatistics execute( DirectStoreAccess stores, Log log )
            throws ConsistencyCheckIncompleteException
    {
        return execute( stores, log, NO_MONITOR );
    }

    ConsistencySummaryStatistics execute( DirectStoreAccess stores, Log log, Monitor reportMonitor )
            throws ConsistencyCheckIncompleteException
    {
        ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();
        InconsistencyReport report = new InconsistencyReport( new InconsistencyMessageLogger( log ), summary );

        OwnerCheck ownerCheck = new OwnerCheck( checkPropertyOwners );
        CountsBuilderDecorator countsBuilder =
                new CountsBuilderDecorator( stores.nativeStores() );
        CheckDecorator decorator = new CheckDecorator.ChainCheckDecorator( ownerCheck, countsBuilder );
        CacheAccess cacheAccess = new DefaultCacheAccess( statistics.getCounts(), threads );
        RecordAccess records = recordAccess( stores.nativeStores(), cacheAccess );
        execute( stores, decorator, records, report, cacheAccess, reportMonitor );
        ownerCheck.scanForOrphanChains( progressFactory );

        if ( checkGraph )
        {
            CountsAccessor countsAccessor = stores.nativeStores().getCounts();
            if ( countsAccessor instanceof CountsTracker )
            {
                CountsTracker tracker = (CountsTracker) countsAccessor;
                try
                {
                    tracker.start();
                }
                catch ( Exception e )
                {
                    // let's hope it was already started :)
                }
            }
            countsBuilder.checkCounts( countsAccessor, new ConsistencyReporter( records, report ), progressFactory );
        }

        if ( !summary.isConsistent() )
        {
            log.warn( "Inconsistencies found: " + summary );
        }
        return summary;
    }

    void execute( final DirectStoreAccess directStoreAccess, final CheckDecorator decorator,
                  final RecordAccess recordAccess, final InconsistencyReport report,
                  CacheAccess cacheAccess, Monitor reportMonitor )
            throws ConsistencyCheckIncompleteException
    {
        final ConsistencyReporter reporter = new ConsistencyReporter( recordAccess, report, reportMonitor );
        StoreProcessor processEverything = new StoreProcessor( decorator, reporter, Stage.SEQUENTIAL_FORWARD, cacheAccess );
        ProgressMonitorFactory.MultiPartBuilder progress = progressFactory.multipleParts(
                "Full Consistency Check" );
        final StoreAccess nativeStores = directStoreAccess.nativeStores();
        try ( IndexAccessors indexes =
                      new IndexAccessors( directStoreAccess.indexes(), nativeStores.getSchemaStore(), samplingConfig ) )
        {
            MultiPassStore.Factory multiPass = new MultiPassStore.Factory(
                    decorator, recordAccess, cacheAccess, report, reportMonitor );
            ConsistencyCheckTasks taskCreator = new ConsistencyCheckTasks( progress, processEverything,
                    nativeStores, statistics, cacheAccess, directStoreAccess.labelScanStore(), indexes,
                    multiPass, reporter, threads );
            List<ConsistencyCheckerTask> tasks =
                    taskCreator.createTasksForFullCheck( checkLabelScanStore, checkIndexes, checkGraph );
            TaskExecutor.execute( tasks, new Runnable()
            {
                @Override
                public void run()
                {
                    decorator.prepare();
                }
            } );
        }
        catch ( Exception e )
        {
            throw new ConsistencyCheckIncompleteException( e );
        }
    }

    static RecordAccess recordAccess( StoreAccess store, CacheAccess cacheAccess )
    {
        return new CacheSmallStoresRecordAccess(
                new DirectRecordAccess( store, cacheAccess ),
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
