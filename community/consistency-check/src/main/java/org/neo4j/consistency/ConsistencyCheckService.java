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
package org.neo4j.consistency;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.newchecker.NodeBasedMemoryLimiter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.statistics.AccessStatistics;
import org.neo4j.consistency.statistics.AccessStatsKeepingStoreAccess;
import org.neo4j.consistency.statistics.DefaultCounts;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.consistency.statistics.VerboseStatistics;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.function.Suppliers;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.index.label.FullStoreChangeStream;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.NativeLabelScanStore;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.DuplicatingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static java.lang.String.format;
import static org.neo4j.consistency.checking.full.ConsistencyFlags.DEFAULT;
import static org.neo4j.consistency.internal.SchemaIndexExtensionLoader.instantiateExtensions;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.io.fs.FileSystemUtils.createOrOpenAsOutputStream;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.TOOL;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;

public class ConsistencyCheckService
{
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
            ProgressMonitorFactory progressFactory, LogProvider logProvider, FileSystemAbstraction fileSystem, boolean verbose, File reportDir,
            ConsistencyFlags consistencyFlags ) throws ConsistencyCheckIncompleteException
    {
        Log log = logProvider.getLog( getClass() );
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        ConfiguringPageCacheFactory pageCacheFactory =
                new ConfiguringPageCacheFactory( fileSystem, config, PageCacheTracer.NULL, logProvider.getLog( PageCache.class ),
                        EmptyVersionContextSupplier.EMPTY, jobScheduler );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        try
        {
            return runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, fileSystem, pageCache, verbose,
                    reportDir, consistencyFlags );
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
            FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose, ConsistencyFlags consistencyFlags )
            throws ConsistencyCheckIncompleteException
    {
        return runFullConsistencyCheck( databaseLayout, config, progressFactory, logProvider, fileSystem, pageCache, verbose,
                defaultReportDir( config ), consistencyFlags );
    }

    public Result runFullConsistencyCheck( DatabaseLayout databaseLayout, Config config,
            ProgressMonitorFactory progressFactory, final LogProvider logProvider, final FileSystemAbstraction fileSystem, final PageCache pageCache,
            final boolean verbose, File reportDir, ConsistencyFlags consistencyFlags ) throws ConsistencyCheckIncompleteException
    {
        assertRecovered( databaseLayout, config, fileSystem );
        Log log = logProvider.getLog( getClass() );
        config.set( GraphDatabaseSettings.read_only, true );
        config.set( GraphDatabaseSettings.pagecache_warmup_enabled, false );

        LifeSupport life = new LifeSupport();
        final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        StoreFactory factory =
                new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fileSystem, logProvider );
        CountsManager countsManager = new CountsManager( pageCache, databaseLayout );
        // Don't start the counts store here as part of life, instead only shut down. This is because it's better to let FullCheck
        // start it and add its missing/broken detection where it can report to user.
        life.add( countsManager );

        ConsistencySummaryStatistics summary;
        final File reportFile = chooseReportPath( reportDir );
        Suppliers.Lazy<PrintWriter> reportWriterSupplier = getReportWriterSupplier( fileSystem, reportFile );
        Log reportLog = new ConsistencyReportLog( reportWriterSupplier );

        // Bootstrap kernel extensions
        Monitors monitors = new Monitors();
        JobScheduler jobScheduler = life.add( JobSchedulerFactory.createInitialisedScheduler() );
        TokenHolders tokenHolders = new TokenHolders( new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        final RecoveryCleanupWorkCollector workCollector = RecoveryCleanupWorkCollector.ignore();
        DatabaseExtensions extensions = life.add( instantiateExtensions( databaseLayout,
                fileSystem, config, new SimpleLogService( logProvider, logProvider ), pageCache, jobScheduler,
                workCollector,
                TOOL, // We use TOOL context because it's true, and also because it uses the 'single' operational mode, which is important.
                monitors, tokenHolders ) );
        DefaultIndexProviderMap indexes = life.add( new DefaultIndexProviderMap( extensions, config ) );

        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            // Load tokens before starting extensions, etc.
            tokenHolders.setInitialTokens( StoreTokens.allReadableTokens( neoStores ) );

            life.start();

            LabelScanStore labelScanStore =
                    new NativeLabelScanStore( pageCache, databaseLayout, fileSystem, FullStoreChangeStream.EMPTY, true, monitors, workCollector );
            life.add( labelScanStore );
            IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout, workCollector, true );
            life.add( indexStatisticsStore );

            int numberOfThreads = defaultConsistencyCheckThreadsNumber();
            Statistics statistics;
            StoreAccess storeAccess;
            AccessStatistics stats = new AccessStatistics();
            if ( verbose )
            {
                statistics = new VerboseStatistics( stats, new DefaultCounts( numberOfThreads ), log );
                storeAccess = new AccessStatsKeepingStoreAccess( neoStores, stats );
            }
            else
            {
                statistics = Statistics.NONE;
                storeAccess = new StoreAccess( neoStores );
            }
            storeAccess.initialize();
            DirectStoreAccess stores = new DirectStoreAccess( storeAccess, labelScanStore, indexes, tokenHolders, indexStatisticsStore, idGeneratorFactory );
            FullCheck check = new FullCheck( progressFactory, statistics, numberOfThreads, consistencyFlags, config, verbose, NodeBasedMemoryLimiter.DEFAULT );
            summary = check.execute( pageCache, stores, countsManager, new DuplicatingLog( log, reportLog ) );
        }
        finally
        {
            life.shutdown();
            if ( reportWriterSupplier.isInitialised() )
            {
                reportWriterSupplier.get().close();
            }
        }

        if ( !summary.isConsistent() )
        {
            log.warn( "See '%s' for a detailed consistency report.", reportFile.getPath() );
            return Result.failure( reportFile, summary );
        }
        return Result.success( reportFile, summary );
    }

    private void assertRecovered( DatabaseLayout databaseLayout, Config config, FileSystemAbstraction fileSystem )
            throws ConsistencyCheckIncompleteException
    {
        try
        {
            if ( isRecoveryRequired( fileSystem, databaseLayout, config ) )
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

    private static Suppliers.Lazy<PrintWriter> getReportWriterSupplier( FileSystemAbstraction fileSystem, File reportFile )
    {
        return Suppliers.lazySingleton( () ->
        {
            try
            {
                return new PrintWriter( createOrOpenAsOutputStream( fileSystem, reportFile, true ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private File chooseReportPath( File reportDir )
    {
        return new File( reportDir, defaultLogFileName( timestamp ) );
    }

    private static File defaultReportDir( Config tuningConfiguration )
    {
        return tuningConfiguration.get( GraphDatabaseSettings.logs_directory ).toFile();
    }

    private static String defaultLogFileName( Date date )
    {
        return format( "inconsistencies-%s.report", new SimpleDateFormat( "yyyy-MM-dd.HH.mm.ss" ).format( date ) );
    }

    public static class Result
    {
        private final boolean successful;
        private final File reportFile;
        private final ConsistencySummaryStatistics summary;

        public static Result failure( File reportFile, ConsistencySummaryStatistics summary )
        {
            return new Result( false, reportFile, summary );
        }

        public static Result success( File reportFile, ConsistencySummaryStatistics summary )
        {
            return new Result( true, reportFile, summary );
        }

        private Result( boolean successful, File reportFile, ConsistencySummaryStatistics summary )
        {
            this.successful = successful;
            this.reportFile = reportFile;
            this.summary = summary;
        }

        public boolean isSuccessful()
        {
            return successful;
        }

        public File reportFile()
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

    private class RebuildPreventingCountsInitializer implements CountsBuilder
    {
        @Override
        public void initialize( CountsAccessor.Updater updater )
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
    private class CountsManager extends LifecycleAdapter implements ThrowingSupplier<CountsStore,IOException>
    {
        private final PageCache pageCache;
        private final DatabaseLayout databaseLayout;
        private GBPTreeCountsStore counts;

        CountsManager( PageCache pageCache, DatabaseLayout databaseLayout )
        {
            this.pageCache = pageCache;
            this.databaseLayout = databaseLayout;
        }

        @Override
        public CountsStore get() throws IOException
        {
            counts = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), RecoveryCleanupWorkCollector.ignore(),
                    new RebuildPreventingCountsInitializer(), true, GBPTreeCountsStore.NO_MONITOR );
            counts.start();
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
