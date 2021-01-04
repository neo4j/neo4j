/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency;

import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checker.NodeBasedMemoryLimiter;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanStore;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.DuplicatingLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.consistency.checking.full.ConsistencyFlags.DEFAULT;
import static org.neo4j.consistency.internal.SchemaIndexExtensionLoader.instantiateExtensions;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.factory.DbmsInfo.TOOL;
import static org.neo4j.kernel.impl.index.schema.FullStoreChangeStream.EMPTY;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;

public class ConsistencyCheckService
{
    private static final String CONSISTENCY_TOKEN_READER_TAG = "consistencyTokenReader";
    private final Date timestamp;

    public ConsistencyCheckService()
    {
        this( new Date() );
    }

    public ConsistencyCheckService( Date timestamp )
    {
        this.timestamp = timestamp;
    }

    @Deprecated
    public Result runFullConsistencyCheck( DatabaseLayout databaseLayout, Config tuningConfiguration,
            ProgressMonitorFactory progressFactory, LogProvider logProvider, boolean verbose )
            throws ConsistencyCheckIncompleteException
    {
        return runFullConsistencyCheck( databaseLayout, tuningConfiguration, progressFactory, logProvider, verbose,
                DEFAULT );
    }

    public Result runFullConsistencyCheck( DatabaseLayout databaseLayout, Config config, ProgressMonitorFactory progressFactory,
            LogProvider logProvider, boolean verbose, ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckIncompleteException
    {
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        try
        {
            return runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, fileSystem, verbose, consistencyFlags );
        }
        finally
        {
            try
            {
                fileSystem.close();
            }
            catch ( IOException e )
            {
                Log log = logProvider.getLog( getClass() );
                log.error( "Failure during shutdown of file system", e );
            }
        }
    }

    public Result runFullConsistencyCheck( DatabaseLayout databaseLayout, Config config,
            ProgressMonitorFactory progressFactory, LogProvider logProvider, FileSystemAbstraction fileSystem, boolean verbose,
            ConsistencyFlags consistencyFlags ) throws ConsistencyCheckIncompleteException
    {
        return runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, fileSystem, verbose,
                defaultReportDir( config ), consistencyFlags );
    }

    public Result runFullConsistencyCheck( DatabaseLayout databaseLayout, Config config,
            ProgressMonitorFactory progressFactory, LogProvider logProvider, FileSystemAbstraction fileSystem, boolean verbose, Path reportDir,
            ConsistencyFlags consistencyFlags ) throws ConsistencyCheckIncompleteException
    {
        Log log = logProvider.getLog( getClass() );
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        var pageCacheTracer = PageCacheTracer.NULL;
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        ConfiguringPageCacheFactory pageCacheFactory =
                new ConfiguringPageCacheFactory( fileSystem, config, pageCacheTracer, logProvider.getLog( PageCache.class ), EmptyVersionContextSupplier.EMPTY,
                        jobScheduler, Clocks.nanoClock(), new MemoryPools( config.get( memory_tracking ) ) );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        try
        {
            return runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, fileSystem, pageCache, verbose,
                    reportDir, consistencyFlags, pageCacheTracer, memoryTracker );
        }
        finally
        {
            try
            {
                pageCache.close();
            }
            catch ( Exception e )
            {
                log.error( "Failure during shutdown of the page cache", e );
            }
            try
            {
                jobScheduler.close();
            }
            catch ( Exception e )
            {
                log.error( "Failure during shutdown of the job scheduler", e );
            }
        }
    }

    public Result runFullConsistencyCheck( DatabaseLayout databaseLayout, Config config, ProgressMonitorFactory progressFactory, LogProvider logProvider,
            FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose, ConsistencyFlags consistencyFlags, PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker ) throws ConsistencyCheckIncompleteException
    {
        return runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, fileSystem, pageCache, verbose,
                defaultReportDir( config ), consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public Result runFullConsistencyCheck( DatabaseLayout databaseLayout, Config config,
            ProgressMonitorFactory progressFactory, final LogProvider logProvider, final FileSystemAbstraction fileSystem, final PageCache pageCache,
            final boolean verbose, Path reportDir, ConsistencyFlags consistencyFlags, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
            throws ConsistencyCheckIncompleteException
    {
        assertRecovered( databaseLayout, config, fileSystem, memoryTracker );
        Log log = logProvider.getLog( getClass() );
        config.set( GraphDatabaseSettings.read_only, true );
        config.set( GraphDatabaseSettings.pagecache_warmup_enabled, false );

        LifeSupport life = new LifeSupport();
        final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        StoreFactory factory =
                new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fileSystem, logProvider, pageCacheTracer );
        CountsManager countsManager = new CountsManager( pageCache, fileSystem, databaseLayout, pageCacheTracer, memoryTracker );
        // Don't start the counts store here as part of life, instead only shut down. This is because it's better to let FullCheck
        // start it and add its missing/broken detection where it can report to user.
        life.add( countsManager );

        ConsistencySummaryStatistics summary;
        final Path reportFile = chooseReportPath( reportDir );

        Log4jLogProvider reportLogProvider = new Log4jLogProvider(
                LogConfig.createBuilder( fileSystem, reportFile, Level.INFO ).createOnDemand().withCategory( false ).build() );
        Log reportLog = reportLogProvider.getLog( getClass() );

        // Bootstrap kernel extensions
        Monitors monitors = new Monitors();
        JobScheduler jobScheduler = life.add( JobSchedulerFactory.createInitialisedScheduler() );
        TokenHolders tokenHolders = new TokenHolders( new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        final RecoveryCleanupWorkCollector workCollector = RecoveryCleanupWorkCollector.ignore();
        DatabaseExtensions extensions = life.add( instantiateExtensions( databaseLayout,
                fileSystem, config, new SimpleLogService( logProvider ), pageCache, jobScheduler,
                workCollector,
                TOOL, // We use TOOL context because it's true, and also because it uses the 'single' operational mode, which is important.
                monitors, tokenHolders ) );
        DefaultIndexProviderMap indexes = life.add( new DefaultIndexProviderMap( extensions, config ) );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            // Load tokens before starting extensions, etc.
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( CONSISTENCY_TOKEN_READER_TAG ) )
            {
                tokenHolders.setInitialTokens( StoreTokens.allReadableTokens( neoStores ), cursorTracer );
            }

            life.start();

            LabelScanStore labelScanStore = TokenScanStore.labelScanStore( pageCache, databaseLayout, fileSystem, EMPTY, true, monitors, workCollector,
                    config, pageCacheTracer, memoryTracker );
            RelationshipTypeScanStore relationshipTypeScanstore = TokenScanStore.toggledRelationshipTypeScanStore( pageCache, databaseLayout, fileSystem,
                    EMPTY, true, monitors, workCollector, config, pageCacheTracer, memoryTracker );
            life.add( labelScanStore );
            life.add( relationshipTypeScanstore );
            IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout, workCollector, true, pageCacheTracer );
            life.add( indexStatisticsStore );

            int numberOfThreads = defaultConsistencyCheckThreadsNumber();
            DirectStoreAccess stores =
                    new DirectStoreAccess( neoStores, labelScanStore, relationshipTypeScanstore, indexes, tokenHolders, indexStatisticsStore,
                            idGeneratorFactory );
            FullCheck check = new FullCheck( progressFactory, numberOfThreads, consistencyFlags, config, verbose, NodeBasedMemoryLimiter.DEFAULT );
            summary = check.execute( pageCache, stores, countsManager, null, pageCacheTracer, memoryTracker, new DuplicatingLog( log, reportLog ) );
        }
        finally
        {
            life.shutdown();
            reportLogProvider.close();
        }

        if ( !summary.isConsistent() )
        {
            log.warn( "See '%s' for a detailed consistency report.", reportFile );
            return Result.failure( reportFile, summary );
        }
        return Result.success( reportFile, summary );
    }

    private void assertRecovered( DatabaseLayout databaseLayout, Config config, FileSystemAbstraction fileSystem, MemoryTracker memoryTracker )
            throws ConsistencyCheckIncompleteException
    {
        try
        {
            if ( isRecoveryRequired( fileSystem, databaseLayout, config, memoryTracker ) )
            {
                throw new IllegalStateException(
                        joinAsLines( "Active logical log detected, this might be a source of inconsistencies.", "Please recover database.",
                                "To perform recovery please start database in single mode and perform clean shutdown." ) );
            }
        }
        catch ( Exception e )
        {
            throw new ConsistencyCheckIncompleteException( e );
        }
    }

    private Path chooseReportPath( Path reportDir )
    {
        return reportDir.resolve( defaultLogFileName( timestamp ) );
    }

    private static Path defaultReportDir( Config tuningConfiguration )
    {
        return tuningConfiguration.get( GraphDatabaseSettings.logs_directory );
    }

    private static String defaultLogFileName( Date date )
    {
        return format( "inconsistencies-%s.report", new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( date ) );
    }

    public static class Result
    {
        private final boolean successful;
        private final Path reportFile;
        private final ConsistencySummaryStatistics summary;

        public static Result failure( Path reportFile, ConsistencySummaryStatistics summary )
        {
            return new Result( false, reportFile, summary );
        }

        public static Result success( Path reportFile, ConsistencySummaryStatistics summary )
        {
            return new Result( true, reportFile, summary );
        }

        private Result( boolean successful, Path reportFile, ConsistencySummaryStatistics summary )
        {
            this.successful = successful;
            this.reportFile = reportFile;
            this.summary = summary;
        }

        public boolean isSuccessful()
        {
            return successful;
        }

        public Path reportFile()
        {
            return reportFile;
        }

        public ConsistencySummaryStatistics summary()
        {
            return summary;
        }
    }

    public static int defaultConsistencyCheckThreadsNumber()
    {
        return Runtime.getRuntime().availableProcessors();
    }

    private static class RebuildPreventingCountsInitializer implements CountsBuilder
    {
        @Override
        public void initialize( CountsAccessor.Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            throw new UnsupportedOperationException( "Counts store needed rebuild, consistency checker will instead report broken or missing counts store" );
        }

        @Override
        public long lastCommittedTxId()
        {
            return 0;
        }
    }

    /**
     * This weird little thing exists because we want to provide {@link CountsStore} from outside checker, but we want to actually instantiate
     * and start it inside the checker where we have the report instance available. So we pass in something that can supply the store...
     * and it can also close it (we do here in {@link ConsistencyCheckService}.
     */
    private static class CountsManager extends LifecycleAdapter implements ThrowingSupplier<CountsStore,IOException>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fileSystem;
        private final DatabaseLayout databaseLayout;
        private final PageCacheTracer pageCacheTracer;
        private final MemoryTracker memoryTracker;
        private GBPTreeCountsStore counts;

        CountsManager( PageCache pageCache, FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCacheTracer pageCacheTracer,
                MemoryTracker memoryTracker )
        {
            this.pageCache = pageCache;
            this.fileSystem = fileSystem;
            this.databaseLayout = databaseLayout;
            this.pageCacheTracer = pageCacheTracer;
            this.memoryTracker = memoryTracker;
        }

        @Override
        public CountsStore get() throws IOException
        {
            counts = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), fileSystem,
                    RecoveryCleanupWorkCollector.ignore(), new RebuildPreventingCountsInitializer(), true, pageCacheTracer, GBPTreeCountsStore.NO_MONITOR );
            counts.start( NULL, memoryTracker );
            return counts;
        }

        @Override
        public void shutdown()
        {
            if ( counts != null )
            {
                counts.close();
            }
        }
    }
}
