/*
 * Copyright (c) "Neo4j"
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

import java.io.OutputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.logging.LoggingReporterFactoryInvocationHandler;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.DuplicatingLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.consistency.internal.SchemaIndexExtensionLoader.instantiateExtensions;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.factory.DbmsInfo.TOOL;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;

public class ConsistencyCheckService
{
    private final Date timestamp;
    private final DatabaseLayout layout;
    private final Config config;
    private final OutputStream progressOutput;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final boolean verbose;
    private final Path reportDir;
    private final ConsistencyFlags consistencyFlags;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;

    public ConsistencyCheckService( DatabaseLayout layout )
    {
        this( new Date(), layout, Config.defaults(), null, NullLogProvider.getInstance(), new DefaultFileSystemAbstraction(), null, false, null,
                ConsistencyFlags.DEFAULT, PageCacheTracer.NULL, EmptyMemoryTracker.INSTANCE );
    }

    private ConsistencyCheckService( Date timestamp, DatabaseLayout layout, Config config, OutputStream progressOutput, LogProvider logProvider,
            FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose, Path reportDir, ConsistencyFlags consistencyFlags,
            PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        this.timestamp = timestamp;
        this.layout = layout;
        this.config = config;
        this.progressOutput = progressOutput;
        this.logProvider = logProvider;
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.verbose = verbose;
        this.reportDir = reportDir;
        this.consistencyFlags = consistencyFlags;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
    }

    public ConsistencyCheckService with( Date timestamp )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( DatabaseLayout layout )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( Config config )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( OutputStream progressOutput )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( LogProvider logProvider )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( FileSystemAbstraction fileSystem )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( PageCache pageCache )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService verbose( boolean verbose )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( Path reportDir )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( ConsistencyFlags consistencyFlags )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( PageCacheTracer pageCacheTracer )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public ConsistencyCheckService with( MemoryTracker memoryTracker )
    {
        return new ConsistencyCheckService( timestamp, layout, config, progressOutput, logProvider, fileSystem, pageCache, verbose, reportDir,
                consistencyFlags, pageCacheTracer, memoryTracker );
    }

    public Result runFullConsistencyCheck()
            throws ConsistencyCheckIncompleteException
    {
        var life = new LifeSupport();
        try
        {
            PageCache pageCache = this.pageCache;
            if ( pageCache == null )
            {
                JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                life.add( onShutdown( jobScheduler::close ) );
                ConfiguringPageCacheFactory pageCacheFactory =
                        new ConfiguringPageCacheFactory( fileSystem, config, pageCacheTracer, logProvider.getLog( PageCache.class ),
                                jobScheduler, Clocks.nanoClock(), new MemoryPools( config.get( memory_tracking ) ),
                                pageCacheConfig -> pageCacheConfig.faultLockStriping( 1 << 11 ) );
                pageCache = pageCacheFactory.getOrCreatePageCache();
                life.add( onShutdown( pageCache::close ) );
            }
            Path reportDir = this.reportDir != null ? this.reportDir : defaultReportDir( config );

            // assert recovered
            var storageEngineFactory = StorageEngineFactory.selectStorageEngine( fileSystem, layout, pageCache ).orElseThrow();
            assertRecovered( layout, config, fileSystem, memoryTracker );
            config.set( GraphDatabaseSettings.pagecache_warmup_enabled, false );

            // instantiate the inconsistencies report logging
            var outLog = logProvider.getLog( getClass() );
            var reportFile = chooseReportPath( reportDir );
            var reportLogProvider =
                    new Log4jLogProvider( LogConfig.createBuilder( fileSystem, reportFile, Level.INFO ).createOnDemand().withCategory( false ).build() );
            life.add( onShutdown( reportLogProvider::close ) );
            var reportLog = reportLogProvider.getLog( getClass() );
            var log = new DuplicatingLog( outLog, reportLog );

            // instantiate kernel extensions and the StaticIndexProviderMapFactory thing
            var jobScheduler = life.add( JobSchedulerFactory.createInitialisedScheduler() );
            var recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.ignore();
            var monitors = new Monitors();
            var tokenHolders = storageEngineFactory.loadReadOnlyTokens( fileSystem, layout, config, pageCache, true, pageCacheTracer );
            var extensions = life.add( instantiateExtensions(
                    layout, fileSystem, config, new SimpleLogService( logProvider ), pageCache, jobScheduler,
                    recoveryCleanupWorkCollector,
                    TOOL,// We use TOOL context because it's true, and also because it uses the 'single' operational mode, which is important.
                    monitors, tokenHolders, pageCacheTracer, readOnly() ) );
            var indexProviders = life.add( StaticIndexProviderMapFactory.create(
                    life, config, pageCache, fileSystem, new SimpleLogService( logProvider ), monitors, readOnly(), TOOL, recoveryCleanupWorkCollector,
                    pageCacheTracer, layout, tokenHolders, jobScheduler, extensions ) );

            // do the consistency check
            life.start();
            var numberOfThreads = defaultConsistencyCheckThreadsNumber();
            var memoryLimitLeewayFactor = config.get( GraphDatabaseInternalSettings.consistency_check_memory_limit_factor );
            var summary = new ConsistencySummaryStatistics();

            if ( consistencyFlags.isCheckIndexStructure() )
            {
                var indexStatisticsStore = life.add( new IndexStatisticsStore( pageCache, layout, recoveryCleanupWorkCollector, readOnly(), pageCacheTracer ) );
                consistencyCheckSingleCheckable( log, summary, indexStatisticsStore, "INDEX_STATISTICS", NULL );
            }

            try
            {
                storageEngineFactory.consistencyCheck( fileSystem, layout, config, pageCache, indexProviders, log, summary, numberOfThreads,
                        memoryLimitLeewayFactor, progressOutput, verbose, consistencyFlags, pageCacheTracer );
            }
            catch ( Exception e )
            {
                throw new ConsistencyCheckIncompleteException( e );
            }

            if ( !summary.isConsistent() )
            {
                log.warn( "Inconsistencies found: " + summary );
                log.warn( "See '%s' for a detailed consistency report.", reportFile );
                return Result.failure( reportFile, summary );
            }
            return Result.success( reportFile, summary );
        }
        finally
        {
            life.shutdown();
        }
    }

    private boolean consistencyCheckSingleCheckable( Log log, ConsistencySummaryStatistics summary, ConsistencyCheckable checkable,
            String type, CursorContext cursorContext )
    {
        LoggingReporterFactoryInvocationHandler handler = new LoggingReporterFactoryInvocationHandler( log, true );
        ReporterFactory proxyFactory = new ReporterFactory( handler );

        boolean consistent = checkable.consistencyCheck( proxyFactory, cursorContext );
        summary.update( type, handler.errors(), handler.warnings() );
        return consistent;
    }

    private static void assertRecovered( DatabaseLayout databaseLayout, Config config, FileSystemAbstraction fileSystem, MemoryTracker memoryTracker )
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
}
