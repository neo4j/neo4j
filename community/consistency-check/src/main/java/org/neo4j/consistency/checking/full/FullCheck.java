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
import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.newchecker.NodeBasedMemoryLimiter;
import org.neo4j.consistency.newchecker.RecordStorageConsistencyChecker;
import org.neo4j.consistency.report.ConsistencyReporter;
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
import org.neo4j.io.pagecache.PageCache;
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

import static org.neo4j.configuration.GraphDatabaseSettings.experimental_consistency_checker;
import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public class FullCheck
{
    private final boolean useExperimentalChecker;
    private final Config config;
    private final boolean verbose;
    private final NodeBasedMemoryLimiter.Factory memoryLimit;
    private final ProgressMonitorFactory progressFactory;
    private final ConsistencyFlags flags;
    private final IndexSamplingConfig samplingConfig;
    private final int threads;
    private final Statistics statistics;

    public FullCheck( ProgressMonitorFactory progressFactory, Statistics statistics, int threads,
                      ConsistencyFlags consistencyFlags, Config config, boolean verbose, NodeBasedMemoryLimiter.Factory memoryLimit )
    {
        this.statistics = statistics;
        this.threads = threads;
        this.progressFactory = progressFactory;
        this.flags = consistencyFlags;
        this.samplingConfig = new IndexSamplingConfig( config );
        this.config = config;
        this.useExperimentalChecker = config.get( experimental_consistency_checker );
        this.verbose = verbose;
        this.memoryLimit = memoryLimit;
    }

    public ConsistencySummaryStatistics execute( PageCache pageCache, DirectStoreAccess stores, ThrowingSupplier<CountsStore,IOException> countsSupplier,
            Log log ) throws ConsistencyCheckIncompleteException
    {
        ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();
        InconsistencyReport report = new InconsistencyReport( new InconsistencyMessageLogger( log ), summary );
        CountsStore countsStore = getCountsStore( countsSupplier, log, summary );
        execute( pageCache, stores, report, countsStore );

        if ( !summary.isConsistent() )
        {
            log.warn( "Inconsistencies found: " + summary );
        }
        return summary;
    }

    private void checkCountsStoreConsistency( InconsistencyReport report, CountsBuilderDecorator countsBuilder, RecordAccess records, CountsStore countsStore )
    {
        if ( flags.isCheckGraph() && countsStore != CountsStore.nullInstance )
        {
            countsBuilder.checkCounts( countsStore, new ConsistencyReporter( records, report ), progressFactory );
        }
    }

    private CountsStore getCountsStore( ThrowingSupplier<CountsStore,IOException> countsSupplier, Log log, ConsistencySummaryStatistics summary )
    {
        // Perhaps other read-only use cases thinks it's fine to just rebuild an in-memory counts store,
        // but the consistency checker should instead prevent rebuild and report that the counts store is broken or missing
        CountsStore countsStore = CountsStore.nullInstance;
        if ( flags.isCheckGraph() || flags.isCheckIndexStructure() )
        {
            try
            {
                countsStore = countsSupplier.get();
            }
            catch ( Exception e )
            {
                log.error( "Counts store is missing, broken or of an older format and will not be consistency checked", e );
                summary.update( RecordType.COUNTS, 1, 0 );
            }
        }
        return countsStore;
    }

    void execute( PageCache pageCache, final DirectStoreAccess directStoreAccess, final InconsistencyReport report, CountsStore countsStore )
            throws ConsistencyCheckIncompleteException
    {
        try ( IndexAccessors indexes = new IndexAccessors( directStoreAccess.indexes(), directStoreAccess.nativeStores().getRawNeoStores(), samplingConfig ) )
        {
            if ( flags.isCheckIndexStructure() )
            {
                consistencyCheckIndexStructure( directStoreAccess.labelScanStore(), directStoreAccess.indexStatisticsStore(), countsStore, indexes,
                        allIdGenerators( directStoreAccess ), report, progressFactory );
            }

            if ( !useExperimentalChecker )
            {
                CacheAccess cacheAccess =
                        new DefaultCacheAccess( DefaultCacheAccess.defaultByteArray( directStoreAccess.nativeStores().getNodeStore().getHighId() ),
                                statistics.getCounts(), threads );
                RecordAccess recordAccess = recordAccess( directStoreAccess.nativeStores(), cacheAccess );
                OwnerCheck ownerCheck = new OwnerCheck( flags.isCheckPropertyOwners() );
                CountsBuilderDecorator countsBuilder = new CountsBuilderDecorator( directStoreAccess.nativeStores() );
                CheckDecorator decorator = new CheckDecorator.ChainCheckDecorator( ownerCheck, countsBuilder );
                final ConsistencyReporter reporter = new ConsistencyReporter( recordAccess, report, NO_MONITOR );
                final StoreAccess nativeStores = directStoreAccess.nativeStores();
                StoreProcessor processEverything = new StoreProcessor( decorator, reporter, Stage.SEQUENTIAL_FORWARD, cacheAccess );
                ProgressMonitorFactory.MultiPartBuilder progress = progressFactory.multipleParts( "Full Consistency Check" );
                MultiPassStore.Factory multiPass = new MultiPassStore.Factory( decorator, recordAccess, cacheAccess, report, NO_MONITOR );
                ConsistencyCheckTasks taskCreator =
                        new ConsistencyCheckTasks( progress, processEverything, nativeStores, statistics, cacheAccess, directStoreAccess.labelScanStore(),
                                indexes, multiPass, reporter, threads );
                List<ConsistencyCheckerTask> tasks = taskCreator.createTasksForFullCheck( flags.isCheckLabelScanStore(), flags.isCheckIndexes(),
                        flags.isCheckGraph() );
                progress.build();
                TaskExecutor.execute( tasks, decorator::prepare );
                checkCountsStoreConsistency( report, countsBuilder, recordAccess, countsStore );
                ownerCheck.scanForOrphanChains( progressFactory );
            }
            else
            {
                try ( RecordStorageConsistencyChecker checker = new RecordStorageConsistencyChecker( pageCache,
                        directStoreAccess.nativeStores().getRawNeoStores(), countsStore, directStoreAccess.labelScanStore(), indexes, report, progressFactory,
                        config, threads, verbose, flags, memoryLimit ) )
                {
                    checker.check();
                }
            }
        }
        catch ( Exception e )
        {
            throw new ConsistencyCheckIncompleteException( e );
        }
    }

    private List<IdGenerator> allIdGenerators( DirectStoreAccess directStoreAccess )
    {
        List<IdGenerator> idGenerators = new ArrayList<>();
        directStoreAccess.idGeneratorFactory().visit( idGenerators::add );
        return idGenerators;
    }

    private static RecordAccess recordAccess( StoreAccess store, CacheAccess cacheAccess )
    {
        return new CacheSmallStoresRecordAccess(
                new DirectRecordAccess( store, cacheAccess ),
                readAllRecords( PropertyKeyTokenRecord.class, store.getPropertyKeyTokenStore() ),
                readAllRecords( RelationshipTypeTokenRecord.class, store.getRelationshipTypeTokenStore() ),
                readAllRecords( LabelTokenRecord.class, store.getLabelTokenStore() ) );
    }

    private static void consistencyCheckIndexStructure( LabelScanStore labelScanStore,
            IndexStatisticsStore indexStatisticsStore, CountsStore countsStore, IndexAccessors indexes,
            List<IdGenerator> idGenerators, InconsistencyReport report, ProgressMonitorFactory progressMonitorFactory )
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

    private static void consistencyCheckNonSchemaIndexes( InconsistencyReport report, ProgressListener listener,
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

    private static void consistencyCheckSingleCheckable( InconsistencyReport report, ProgressListener listener, ConsistencyCheckable checkable,
            RecordType recordType )
    {
        ConsistencyReporter.FormattingDocumentedHandler handler = ConsistencyReporter.formattingHandler( report, recordType );
        ReporterFactory proxyFactory = new ReporterFactory( handler );

        checkable.consistencyCheck( proxyFactory );
        handler.updateSummary();
        listener.add( 1 );
    }

    private static void consistencyCheckSchemaIndexes( IndexAccessors indexes, InconsistencyReport report, ProgressListener listener )
    {
        List<IndexDescriptor> rulesToRemove = new ArrayList<>();
        for ( IndexDescriptor onlineRule : indexes.onlineRules() )
        {
            ConsistencyReporter.FormattingDocumentedHandler handler = ConsistencyReporter.formattingHandler( report, RecordType.INDEX );
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
