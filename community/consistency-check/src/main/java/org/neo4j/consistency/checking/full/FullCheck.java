/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ByteArrayBitsManipulator;
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
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.annotations.ReporterFactory;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;

public class FullCheck
{
    private final boolean checkPropertyOwners;
    private final boolean checkLabelScanStore;
    private final boolean checkIndexes;
    private final boolean checkIndexStructure;
    private final ProgressMonitorFactory progressFactory;
    private final IndexSamplingConfig samplingConfig;
    private final boolean checkGraph;
    private final int threads;
    private final Statistics statistics;
    private final boolean startCountsStore;

    public FullCheck( Config config, ProgressMonitorFactory progressFactory,
            Statistics statistics, int threads, boolean startCountsStore )
    {
        this( progressFactory, statistics, threads, new ConsistencyFlags( config ), config, startCountsStore );
    }

    public FullCheck( ProgressMonitorFactory progressFactory, Statistics statistics, int threads,
                      ConsistencyFlags consistencyFlags, Config config, boolean startCountsStore )
    {
        this.statistics = statistics;
        this.threads = threads;
        this.progressFactory = progressFactory;
        this.samplingConfig = new IndexSamplingConfig( config );
        this.checkGraph = consistencyFlags.isCheckGraph();
        this.checkIndexes = consistencyFlags.isCheckIndexes();
        this.checkIndexStructure = consistencyFlags.isCheckIndexStructure();
        this.checkLabelScanStore = consistencyFlags.isCheckLabelScanStore();
        this.checkPropertyOwners = consistencyFlags.isCheckPropertyOwners();
        this.startCountsStore = startCountsStore;
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
        CacheAccess cacheAccess = new DefaultCacheAccess(
                AUTO_WITHOUT_PAGECACHE.newByteArray( stores.nativeStores().getNodeStore().getHighId(), new byte[ByteArrayBitsManipulator.MAX_BYTES] ),
                statistics.getCounts(), threads );
        RecordAccess records = recordAccess( stores.nativeStores(), cacheAccess );
        execute( stores, decorator, records, report, cacheAccess, reportMonitor );
        ownerCheck.scanForOrphanChains( progressFactory );

        if ( checkGraph )
        {
            CountsAccessor countsAccessor = stores.nativeStores().getCounts();
            boolean checkCounts = true;
            if ( startCountsStore && countsAccessor instanceof CountsTracker )
            {
                CountsTracker tracker = (CountsTracker) countsAccessor;
                // Perhaps other read-only use cases thinks it's fine to just rebuild an in-memory counts store,
                // but the consistency checker should instead prevent rebuild and report that the counts store is broken or missing
                tracker.setInitializer( new RebuildPreventingCountsInitializer() );
                try
                {
                    tracker.start();
                }
                catch ( Exception e )
                {
                    log.error( "Counts store is missing, broken or of an older format and will not be consistency checked", e );
                    summary.update( RecordType.COUNTS, 1, 0 );
                    checkCounts = false;
                }
            }

            if ( checkCounts )
            {
                countsBuilder.checkCounts( countsAccessor, new ConsistencyReporter( records, report ), progressFactory );
            }
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
                    nativeStores, statistics, cacheAccess, directStoreAccess.labelScanStore(), indexes, directStoreAccess.tokenHolders(),
                    multiPass, reporter, threads );

            if ( checkIndexStructure )
            {
                consistencyCheckIndexStructure( directStoreAccess.labelScanStore(), indexes, reporter, progressFactory );
            }

            List<ConsistencyCheckerTask> tasks =
                    taskCreator.createTasksForFullCheck( checkLabelScanStore, checkIndexes, checkGraph );
            progress.build();
            TaskExecutor.execute( tasks, decorator::prepare );
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

    private static void consistencyCheckIndexStructure( LabelScanStore labelScanStore, IndexAccessors indexes,
            ConsistencyReporter report, ProgressMonitorFactory progressMonitorFactory )
    {
        final long schemaIndexCount = Iterables.count( indexes.onlineRules() );
        final long additionalCount = 1; // LabelScanStore
        final long totalCount = schemaIndexCount + additionalCount;
        final ProgressListener listener = progressMonitorFactory.singlePart( "Index structure consistency check", totalCount );
        listener.started();

        consistencyCheckLabelScanStore( labelScanStore, report, listener );
        consistencyCheckSchemaIndexes( indexes, report, listener );

        listener.done();
    }

    private static void consistencyCheckLabelScanStore( LabelScanStore labelScanStore, ConsistencyReporter report, ProgressListener listener )
    {
        ConsistencyReporter.FormattingDocumentedHandler handler = report.formattingHandler( RecordType.LABEL_SCAN_DOCUMENT );
        ReporterFactory proxyFactory = new ReporterFactory( handler );
        labelScanStore.consistencyCheck( proxyFactory );
        handler.updateSummary();
        listener.add( 1 );
    }

    private static void consistencyCheckSchemaIndexes( IndexAccessors indexes, ConsistencyReporter report, ProgressListener listener )
    {
        List<StoreIndexDescriptor> rulesToRemove = new ArrayList<>();
        for ( StoreIndexDescriptor onlineRule : indexes.onlineRules() )
        {
            ConsistencyReporter.FormattingDocumentedHandler handler = report.formattingHandler( RecordType.INDEX );
            ReporterFactory reporterFactory = new ReporterFactory( handler );
            IndexAccessor accessor = indexes.accessorFor( onlineRule );
            if ( !accessor.consistencyCheck( reporterFactory ) )
            {
                rulesToRemove.add( onlineRule );
            }
            handler.updateSummary();
            listener.add( 1 );
        }
        for ( StoreIndexDescriptor toRemove : rulesToRemove )
        {
            indexes.remove( toRemove );
        }
    }

    private static <T extends AbstractBaseRecord> T[] readAllRecords( Class<T> type, RecordStore<T> store )
    {
        @SuppressWarnings( "unchecked" )
        T[] records = (T[]) Array.newInstance( type, (int) store.getHighId() );
        for ( int i = 0; i < records.length; i++ )
        {
            records[i] = store.getRecord( i, store.newRecord(), FORCE );
        }
        return records;
    }

    private class RebuildPreventingCountsInitializer implements DataInitializer<CountsAccessor.Updater>
    {
        @Override
        public void initialize( CountsAccessor.Updater updater )
        {
            throw new UnsupportedOperationException( "Counts store needed rebuild, consistency checker will instead report broken or missing counts store" );
        }

        @Override
        public long initialVersion()
        {
            return 0;
        }
    }
}
