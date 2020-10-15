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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.TransactionLogVersionProvider;
import org.neo4j.dbms.database.TransactionLogVersionProviderImpl;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.TransactionLogVersionSelector;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.api.TransactionIdStore;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;

/**
 * Transactional log files facade class builder.
 * Depending from required abilities user can choose what kind of facade instance is required: from fully functional
 * to simplified that can operate only based on available log files without accessing stores and other external
 * components.
 * <br/>
 * Builder allow to configure any dependency explicitly and will use default value if that exist otherwise.
 * More specific dependencies always take precedence over more generic.
 * <br/>
 * For example: provided rotation threshold will
 * be used in precedence of value that can be specified in provided config.
 */
public class LogFilesBuilder
{
    private static final String READ_ONLY_TRANSACTION_STORE_READER_TAG = "readOnlyTransactionStoreReader";
    private static final String READ_ONLY_LOG_VERSION_READER_TAG = "readOnlyLogVersionReader";

    private boolean readOnly;
    private PageCache pageCache;
    private CommandReaderFactory commandReaderFactory = CommandReaderFactory.NO_COMMANDS;
    private DatabaseLayout databaseLayout;
    private Path logsDirectory;
    private Config config;
    private Long rotationThreshold;
    private LogEntryReader logEntryReader;
    private LogProvider logProvider = NullLogProvider.getInstance();
    private DependencyResolver dependencies;
    private FileSystemAbstraction fileSystem;
    private LogVersionRepository logVersionRepository;
    private TransactionIdStore transactionIdStore;
    private LongSupplier lastCommittedTransactionIdSupplier;
    private Supplier<LogPosition> lastClosedPositionSupplier;
    private boolean fileBasedOperationsOnly;
    private DatabaseTracers databaseTracers = DatabaseTracers.EMPTY;
    private MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
    private DatabaseHealth databaseHealth;
    private Clock clock;
    private Monitors monitors;
    private StoreId storeId;
    private NativeAccess nativeAccess;
    private TransactionLogVersionProvider transactionLogVersionProvider;

    private LogFilesBuilder()
    {
    }

    /**
     * Builder for fully functional transactional log files.
     * Log files will be able to access store and external components information, perform rotations, etc.
     * @param databaseLayout database directory
     * @param fileSystem log files filesystem
     */
    public static LogFilesBuilder builder( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem )
    {
        LogFilesBuilder filesBuilder = new LogFilesBuilder();
        filesBuilder.databaseLayout = databaseLayout;
        filesBuilder.fileSystem = fileSystem;
        return filesBuilder;
    }

    /**
     * Build log files that can access and operate only on active set of log files without ability to
     * rotate and create any new one. Appending to current log file still possible.
     * Store and external components access available in read only mode.
     *
     * @param databaseLayout store directory
     * @param fileSystem log file system
     * @param pageCache page cache for read only store info access
     */
    public static LogFilesBuilder activeFilesBuilder( DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem, PageCache pageCache )
    {
        LogFilesBuilder builder = builder( databaseLayout, fileSystem );
        builder.pageCache = pageCache;
        builder.readOnly = true;
        return builder;
    }

    /**
     * Build log files that will be able to perform only operations on a log files directly.
     * Any operation that will require access to a store or other parts of runtime will fail.
     * Should be mainly used only for testing purposes or when only file based operations will be performed
     * @param logsDirectory log files directory
     * @param fileSystem file system
     */
    public static LogFilesBuilder logFilesBasedOnlyBuilder( Path logsDirectory, FileSystemAbstraction fileSystem )
    {
        LogFilesBuilder builder = new LogFilesBuilder();
        builder.logsDirectory = logsDirectory;
        builder.fileSystem = fileSystem;
        builder.fileBasedOperationsOnly = true;
        return builder;
    }

    public LogFilesBuilder withLastClosedTransactionPositionSupplier( Supplier<LogPosition> lastClosedPositionSupplier )
    {
        this.lastClosedPositionSupplier = lastClosedPositionSupplier;
        return this;
    }

    public LogFilesBuilder withLogVersionRepository( LogVersionRepository logVersionRepository )
    {
        this.logVersionRepository = logVersionRepository;
        return this;
    }

    public LogFilesBuilder withTransactionLogVersionProvider( TransactionLogVersionProvider transactionLogVersionProvider )
    {
        this.transactionLogVersionProvider = transactionLogVersionProvider;
        return this;
    }

    public LogFilesBuilder withTransactionIdStore( TransactionIdStore transactionIdStore )
    {
        this.transactionIdStore = transactionIdStore;
        return this;
    }

    public LogFilesBuilder withLogProvider( LogProvider logProvider )
    {
        this.logProvider = logProvider;
        return this;
    }

    public LogFilesBuilder withLastCommittedTransactionIdSupplier( LongSupplier transactionIdSupplier )
    {
        this.lastCommittedTransactionIdSupplier = transactionIdSupplier;
        return this;
    }

    public LogFilesBuilder withLogEntryReader( LogEntryReader logEntryReader )
    {
        this.logEntryReader = logEntryReader;
        return this;
    }

    public LogFilesBuilder withConfig( Config config )
    {
        this.config = config;
        return this;
    }

    public LogFilesBuilder withMonitors( Monitors monitors )
    {
        this.monitors = monitors;
        return this;
    }

    public LogFilesBuilder withRotationThreshold( long rotationThreshold )
    {
        this.rotationThreshold = rotationThreshold;
        return this;
    }

    public LogFilesBuilder withDependencies( DependencyResolver dependencies )
    {
        this.dependencies = dependencies;
        return this;
    }

    public LogFilesBuilder withDatabaseTracers( DatabaseTracers databaseTracers )
    {
        this.databaseTracers = databaseTracers;
        return this;
    }

    public LogFilesBuilder withMemoryTracker( MemoryTracker memoryTracker )
    {
        this.memoryTracker = memoryTracker;
        return this;
    }

    public LogFilesBuilder withNativeAccess( NativeAccess nativeAccess )
    {
        this.nativeAccess = nativeAccess;
        return this;
    }

    public LogFilesBuilder withStoreId( StoreId storeId )
    {
        this.storeId = storeId;
        return this;
    }

    public LogFilesBuilder withClock( Clock clock )
    {
        this.clock = clock;
        return this;
    }

    public LogFilesBuilder withDatabaseHealth( DatabaseHealth databaseHealth )
    {
        this.databaseHealth = databaseHealth;
        return this;
    }

    public LogFilesBuilder withCommandReaderFactory( CommandReaderFactory commandReaderFactory )
    {
        this.commandReaderFactory = commandReaderFactory;
        return this;
    }

    public LogFilesBuilder withLogsDirectory( Path logsDirectory )
    {
        this.logsDirectory = logsDirectory;
        return this;
    }

    public LogFiles build() throws IOException
    {
        TransactionLogFilesContext filesContext = buildContext();
        Path logsDirectory = getLogsDirectory();
        filesContext.getFileSystem().mkdirs( logsDirectory );
        return new TransactionLogFiles( logsDirectory, TransactionLogFilesHelper.DEFAULT_NAME, filesContext );
    }

    private Path getLogsDirectory()
    {
        return requireNonNullElseGet( logsDirectory, () -> databaseLayout.getTransactionLogsDirectory() );
    }

    TransactionLogFilesContext buildContext() throws IOException
    {
        if ( logEntryReader == null )
        {
            requireNonNull( commandReaderFactory );
            logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );
        }
        if ( config == null )
        {
            config = Config.defaults();
        }
        requireNonNull( fileSystem );
        Supplier<StoreId> storeIdSupplier = getStoreId();
        Supplier<LogVersionRepository> logVersionRepositorySupplier = getLogVersionRepositorySupplier();
        LongSupplier lastCommittedIdSupplier = lastCommittedIdSupplier();
        LongSupplier committingTransactionIdSupplier = committingIdSupplier();
        Supplier<LogPosition> lastClosedTransactionPositionSupplier = closePositionSupplier();

        // Register listener for rotation threshold
        AtomicLong rotationThreshold = getRotationThresholdAndRegisterForUpdates();
        AtomicBoolean tryPreallocateTransactionLogs = getTryToPreallocateTransactionLogs();
        var nativeAccess = getNativeAccess();
        var monitors = getMonitors();
        var health = getDatabaseHealth();
        var clock = getClock();

        // If no transaction log version provider has been supplied explicitly, we try to use the version from the system database.
        // Or the latest version if we can't find the system db version.
        if ( transactionLogVersionProvider == null )
        {
            transactionLogVersionProvider = TransactionLogVersionSelector.LATEST::version;
            if ( dependencies != null )
            {
                try
                {
                    DbmsRuntimeRepository dbmsRuntimeRepository = dependencies.resolveDependency( DbmsRuntimeRepository.class );
                    transactionLogVersionProvider = new TransactionLogVersionProviderImpl( dbmsRuntimeRepository );
                }
                catch ( UnsatisfiedDependencyException e )
                {
                    // Use latest version if can't find version in system db.
                }
            }
        }

        return new TransactionLogFilesContext( rotationThreshold, tryPreallocateTransactionLogs, logEntryReader, lastCommittedIdSupplier,
                committingTransactionIdSupplier, lastClosedTransactionPositionSupplier, logVersionRepositorySupplier,
                fileSystem, logProvider, databaseTracers, storeIdSupplier, nativeAccess, memoryTracker, monitors, config.get( fail_on_corrupted_log_files ),
                health, transactionLogVersionProvider, clock, config );
    }

    private Clock getClock()
    {
        if ( clock != null )
        {
            return clock;
        }
        return Clock.systemUTC();
    }

    private DatabaseHealth getDatabaseHealth()
    {
        if ( databaseHealth != null )
        {
            return databaseHealth;
        }
        if ( dependencies != null )
        {
            return dependencies.resolveDependency( DatabaseHealth.class );
        }
        return new DatabaseHealth( PanicEventGenerator.NO_OP, logProvider.getLog( DatabaseHealth.class ) );
    }

    private Monitors getMonitors()
    {
        if ( monitors == null )
        {
            return new Monitors();
        }
        return monitors;
    }

    private NativeAccess getNativeAccess()
    {
        if ( nativeAccess != null )
        {
            return nativeAccess;
        }
        return NativeAccessProvider.getNativeAccess();
    }

    private AtomicLong getRotationThresholdAndRegisterForUpdates()
    {
        if ( rotationThreshold != null )
        {
            return new AtomicLong( rotationThreshold );
        }
        if ( readOnly )
        {
            return new AtomicLong( Long.MAX_VALUE );
        }
        AtomicLong configThreshold = new AtomicLong( config.get( logical_log_rotation_threshold ) );
        config.addListener( logical_log_rotation_threshold, ( prev, update ) -> configThreshold.set( update ) );
        return configThreshold;
    }

    private AtomicBoolean getTryToPreallocateTransactionLogs()
    {
        if ( readOnly )
        {
            return new AtomicBoolean( false );
        }
        AtomicBoolean tryToPreallocate = new AtomicBoolean( config.get( preallocate_logical_logs ) );
        config.addListener( preallocate_logical_logs, ( prev, update ) ->
        {
            String logMessage = "Updating " + preallocate_logical_logs.name() + " from " + prev + " to " + update;
            logProvider.getLog( LogFiles.class ).debug( logMessage );
            tryToPreallocate.set( update );
        } );
        return tryToPreallocate;
    }

    private Supplier<LogVersionRepository> getLogVersionRepositorySupplier() throws IOException
    {
        if ( logVersionRepository != null )
        {
            return () -> logVersionRepository;
        }
        if ( fileBasedOperationsOnly )
        {
            return () ->
            {
                throw new UnsupportedOperationException( "Current version of log files can't perform any " +
                    "operation that require availability of log version repository. Please build full version of log files to be able to use them." );
            };
        }
        if ( readOnly )
        {
            requireNonNull( pageCache, "Read only log files require page cache to be able to read current log version." );
            requireNonNull( databaseLayout,"Store directory is required.");
            LogVersionRepository logVersionRepository = readOnlyLogVersionRepository();
            return () -> logVersionRepository;
        }
        else
        {
            requireNonNull( dependencies, LogVersionRepository.class.getSimpleName() + " is required. " +
                    "Please provide an instance or a dependencies where it can be found." );
            return dependencies.provideDependency( LogVersionRepository.class );
        }
    }

    private LongSupplier lastCommittedIdSupplier() throws IOException
    {
        if ( lastCommittedTransactionIdSupplier != null )
        {
            return lastCommittedTransactionIdSupplier;
        }
        if ( transactionIdStore != null )
        {
            return transactionIdStore::getLastCommittedTransactionId;
        }
        if ( fileBasedOperationsOnly )
        {
            return () ->
            {
                throw new UnsupportedOperationException( "Current version of log files can't perform any " +
                        "operation that require availability of transaction id store. Please build full version of log files " +
                        "to be able to use them." );
            };
        }
        if ( readOnly )
        {
            requireNonNull( pageCache, "Read only log files require page cache to be able to read committed " +
                    "transaction info from store store." );
            requireNonNull( databaseLayout, "Store directory is required." );
            TransactionIdStore transactionIdStore = readOnlyTransactionIdStore();
            return transactionIdStore::getLastCommittedTransactionId;
        }
        else
        {
            requireNonNull( dependencies, TransactionIdStore.class.getSimpleName() + " is required. " +
                    "Please provide an instance or a dependencies where it can be found." );
            return () -> resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
        }
    }

    private Supplier<LogPosition> closePositionSupplier() throws IOException
    {
        if ( lastClosedPositionSupplier != null )
        {
            return lastClosedPositionSupplier;
        }
        if ( transactionIdStore != null )
        {
            return () ->
            {
                long[] lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
                return new LogPosition( lastClosedTransaction[1], lastClosedTransaction[2] );
            };
        }
        if ( fileBasedOperationsOnly )
        {
            return () ->
            {
                throw new UnsupportedOperationException( "Current version of log files can't perform any " +
                        "operation that require availability of transaction id store. Please build full version of log files " +
                        "to be able to use them." );
            };
        }
        if ( readOnly )
        {
            requireNonNull( pageCache, "Read only log files require page cache to be able to read committed " +
                    "transaction info from store store." );
            requireNonNull( databaseLayout, "Store directory is required." );
            TransactionIdStore transactionIdStore = readOnlyTransactionIdStore();
            return () ->
            {
                long[] lastClosedTransaction = transactionIdStore.getLastClosedTransaction();
                return new LogPosition( lastClosedTransaction[1], lastClosedTransaction[2] );
            };
        }
        else
        {
            requireNonNull( dependencies, TransactionIdStore.class.getSimpleName() + " is required. " +
                    "Please provide an instance or a dependencies where it can be found." );
            return () -> {
                long[] lastClosedTransaction = resolveDependency( TransactionIdStore.class ).getLastClosedTransaction();
                return new LogPosition( lastClosedTransaction[1], lastClosedTransaction[2] );
            };
        }
    }

    private LongSupplier committingIdSupplier() throws IOException
    {
        if ( transactionIdStore != null )
        {
            return transactionIdStore::committingTransactionId;
        }
        if ( fileBasedOperationsOnly )
        {
            return () ->
            {
                throw new UnsupportedOperationException( "Current version of log files can't perform any " +
                        "operation that require availability of transaction id store. Please build full version of log files " +
                        "to be able to use them." );
            };
        }
        if ( readOnly )
        {
            requireNonNull( pageCache, "Read only log files require page cache to be able to read committed " +
                    "transaction info from store store." );
            requireNonNull( databaseLayout, "Store directory is required." );
            TransactionIdStore transactionIdStore = readOnlyTransactionIdStore();
            return transactionIdStore::committingTransactionId;
        }
        else
        {
            requireNonNull( dependencies, TransactionIdStore.class.getSimpleName() + " is required. " +
                    "Please provide an instance or a dependencies where it can be found." );
            return () -> resolveDependency( TransactionIdStore.class ).committingTransactionId();
        }
    }

    private Supplier<StoreId> getStoreId()
    {
        if ( storeId != null )
        {
            return () -> storeId;
        }
        if ( fileBasedOperationsOnly )
        {
            return () ->
            {
                throw new UnsupportedOperationException( "Current version of log files can't perform any " +
                        "operation that require availability of store id. Please build full version of log files " +
                        "to be able to use them." );
            };
        }
        return () -> resolveDependency( StoreIdProvider.class ).getStoreId();
    }

    private TransactionIdStore readOnlyTransactionIdStore() throws IOException
    {
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        var pageCacheTracer = databaseTracers.getPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( READ_ONLY_TRANSACTION_STORE_READER_TAG ) )
        {
            return storageEngineFactory.readOnlyTransactionIdStore( fileSystem, databaseLayout, pageCache, cursorTracer );
        }
    }

    private LogVersionRepository readOnlyLogVersionRepository() throws IOException
    {
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        var pageCacheTracer = databaseTracers.getPageCacheTracer();
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( READ_ONLY_LOG_VERSION_READER_TAG ) )
        {
            return storageEngineFactory.readOnlyLogVersionRepository( databaseLayout, pageCache, cursorTracer );
        }
    }

    private <T> T resolveDependency( Class<T> clazz )
    {
        return dependencies.resolveDependency( clazz );
    }
}
