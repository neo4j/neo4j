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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.configuration.Config;
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
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.counts.CountsStore;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.Log;

import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

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

    public FullCheck( ConsistencyFlags consistencyFlags, Config config, ProgressMonitorFactory progressFactory,
            Statistics statistics, int threads )
    {
        this( progressFactory, statistics, threads, consistencyFlags, config );
    }

    public FullCheck( ProgressMonitorFactory progressFactory, Statistics statistics, int threads,
                      ConsistencyFlags consistencyFlags, Config config )
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
    }

    public ConsistencySummaryStatistics execute( DirectStoreAccess stores, ThrowingSupplier<CountsStore,IOException> countsSupplier, Log log )
            throws ConsistencyCheckIncompleteException
    {
        return execute( stores, countsSupplier, log, NO_MONITOR );
    }

    ConsistencySummaryStatistics execute( DirectStoreAccess stores, ThrowingSupplier<CountsStore,IOException> countsSupplier, Log log, Monitor reportMonitor )
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
        CountsStore countsStore = checkCountsStore( countsSupplier, log, summary, report, countsBuilder, records );
        execute( stores, decorator, records, report, cacheAccess, reportMonitor, countsStore );
        ownerCheck.scanForOrphanChains( progressFactory );

        if ( !summary.isConsistent() )
        {
            log.warn( "Inconsistencies found: " + summary );
        }
        return summary;
    }

    private CountsStore checkCountsStore( ThrowingSupplier<CountsStore,IOException> countsSupplier, Log log, ConsistencySummaryStatistics summary,
            InconsistencyReport report, CountsBuilderDecorator countsBuilder, RecordAccess records )
    {
        CountsStore countsStore = CountsStore.nullInstance;
        // Perhaps other read-only use cases thinks it's fine to just rebuild an in-memory counts store,
        // but the consistency checker should instead prevent rebuild and report that the counts store is broken or missing
        try
        {
            countsStore = countsSupplier.get();
            if ( checkGraph )
            {
                countsBuilder.checkCounts( countsStore, new ConsistencyReporter( records, report ), progressFactory );
            }
        }
        catch ( Exception e )
        {
            if ( checkGraph )
            {
                log.error( "Counts store is missing, broken or of an older format and will not be consistency checked", e );
                summary.update( RecordType.COUNTS, 1, 0 );
            }
        }
        return countsStore;
    }

    void execute( final DirectStoreAccess directStoreAccess, final CheckDecorator decorator,
            final RecordAccess recordAccess, final InconsistencyReport report,
            CacheAccess cacheAccess, Monitor reportMonitor, CountsStore countsStore )
            throws ConsistencyCheckIncompleteException
    {
        final ConsistencyReporter reporter = new ConsistencyReporter( recordAccess, report, reportMonitor );
        StoreProcessor processEverything = new StoreProcessor( decorator, reporter, Stage.SEQUENTIAL_FORWARD, cacheAccess );
        ProgressMonitorFactory.MultiPartBuilder progress = progressFactory.multipleParts(
                "Full Consistency Check" );
        final StoreAccess nativeStores = directStoreAccess.nativeStores();
        try ( IndexAccessors indexes = new IndexAccessors( directStoreAccess.indexes(), nativeStores, samplingConfig ) )
        {
            MultiPassStore.Factory multiPass = new MultiPassStore.Factory(
                    decorator, recordAccess, cacheAccess, report, reportMonitor );
            ConsistencyCheckTasks taskCreator =
                    new ConsistencyCheckTasks( progress, processEverything, nativeStores, statistics, cacheAccess, directStoreAccess.labelScanStore(), indexes,
                            multiPass, reporter, threads );

            if ( checkIndexStructure )
            {
                consistencyCheckIndexStructure( directStoreAccess.labelScanStore(), directStoreAccess.indexStatisticsStore(), countsStore, indexes,
                        nativeStores.idGenerators(), reporter, progressFactory );
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

    private static void consistencyCheckIndexStructure( LabelScanStore labelScanStore,
            IndexStatisticsStore indexStatisticsStore, CountsStore countsStore, IndexAccessors indexes,
            List<IdGenerator> idGenerators, ConsistencyReporter report, ProgressMonitorFactory progressMonitorFactory )
    {
        final long schemaIndexCount = Iterables.count( indexes.onlineRules() );
        final long additionalCount = 1 /*LabelScanStore*/ + 1 /*IndexStatisticsStore*/ + 1 /*countsStore*/;
        final long idGeneratorsCount = idGenerators.size();
        final long totalCount = schemaIndexCount + additionalCount + idGeneratorsCount;
        final ProgressListener listener = progressMonitorFactory.singlePart( "Index structure consistency check", totalCount );
        listener.started();

        consistencyCheckNonSchemaIndexes( report, listener, labelScanStore, indexStatisticsStore, countsStore, idGenerators );
        consistencyCheckSchemaIndexes( indexes, report, listener );

        listener.done();
    }

    private static void consistencyCheckNonSchemaIndexes( ConsistencyReporter report, ProgressListener listener,
            LabelScanStore labelScanStore, IndexStatisticsStore indexStatisticsStore, CountsStore countsStore, List<IdGenerator> idGenerators )
    {
        consistencyCheckSingleCheckable( report, listener, labelScanStore, RecordType.LABEL_SCAN_DOCUMENT );
        consistencyCheckSingleCheckable( report, listener, indexStatisticsStore, RecordType.INDEX_STATISTICS );
        consistencyCheckSingleCheckable( report, listener, countsStore, RecordType.COUNTS );
        for ( IdGenerator idGenerator : idGenerators )
        {
            consistencyCheckSingleCheckable( report, listener, idGenerator, RecordType.ID_STORE );
        }
    }

    private static void consistencyCheckSingleCheckable( ConsistencyReporter report, ProgressListener listener, ConsistencyCheckable checkable,
            RecordType recordType )
    {
        ConsistencyReporter.FormattingDocumentedHandler handler = report.formattingHandler( recordType );
        ReporterFactory proxyFactory = new ReporterFactory( handler );

        checkable.consistencyCheck( proxyFactory );
        handler.updateSummary();
        listener.add( 1 );
    }

    private static void consistencyCheckSchemaIndexes( IndexAccessors indexes, ConsistencyReporter report, ProgressListener listener )
    {
        List<IndexDescriptor> rulesToRemove = new ArrayList<>();
        for ( IndexDescriptor onlineRule : indexes.onlineRules() )
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
        for ( IndexDescriptor toRemove : rulesToRemove )
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
}
