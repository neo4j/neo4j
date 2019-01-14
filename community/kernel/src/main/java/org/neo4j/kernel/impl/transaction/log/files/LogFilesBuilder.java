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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.Dependencies;

import static java.util.Objects.requireNonNull;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_log_rotation_threshold;

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
    private boolean readOnly;
    private PageCache pageCache;
    private File storeDirectory;
    private File logsDirectory;
    private Config config;
    private Long rotationThreshold;
    private LogEntryReader logEntryReader;
    private LogFileCreationMonitor logFileCreationMonitor;
    private Dependencies dependencies;
    private FileSystemAbstraction fileSystem;
    private LogVersionRepository logVersionRepository;
    private TransactionIdStore transactionIdStore;
    private LongSupplier lastCommittedTransactionIdSupplier;
    private String logFileName = TransactionLogFiles.DEFAULT_NAME;
    private boolean fileBasedOperationsOnly;

    /**
     * Builder for fully functional transactional log files.
     * Log files will be able to access store and external components information, perform rotations, etc.
     * @param storeDirectory store directory
     * @param fileSystem log files filesystem
     */
    public static LogFilesBuilder builder( File storeDirectory, FileSystemAbstraction fileSystem )
    {
        LogFilesBuilder filesBuilder = new LogFilesBuilder();
        filesBuilder.storeDirectory = storeDirectory;
        filesBuilder.fileSystem = fileSystem;
        return filesBuilder;
    }

    /**
     * Build log files that can access and operate only on active set of log files without ability to
     * rotate and create any new one. Appending to current log file still possible.
     * Store and external components access available in read only mode.
     * @param storeDirectory store directory
     * @param fileSystem log file system
     * @param pageCache page cache for read only store info access
     */
    public static LogFilesBuilder activeFilesBuilder( File storeDirectory, FileSystemAbstraction fileSystem, PageCache pageCache )
    {
        LogFilesBuilder builder = builder( storeDirectory, fileSystem );
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
    public static LogFilesBuilder logFilesBasedOnlyBuilder( File logsDirectory, FileSystemAbstraction fileSystem )
    {
        LogFilesBuilder builder = new LogFilesBuilder();
        builder.logsDirectory = logsDirectory;
        builder.fileSystem = fileSystem;
        builder.fileBasedOperationsOnly = true;
        return builder;
    }

    LogFilesBuilder withLogFileName( String name )
    {
        this.logFileName = name;
        return this;
    }

    public LogFilesBuilder withLogVersionRepository( LogVersionRepository logVersionRepository )
    {
        this.logVersionRepository = logVersionRepository;
        return this;
    }

    public LogFilesBuilder withTransactionIdStore( TransactionIdStore transactionIdStore )
    {
        this.transactionIdStore = transactionIdStore;
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

    public LogFilesBuilder withLogFileMonitor( LogFileCreationMonitor logFileCreationMonitor )
    {
        this.logFileCreationMonitor = logFileCreationMonitor;
        return this;
    }

    public LogFilesBuilder withConfig( Config config )
    {
        this.config = config;
        return this;
    }

    public LogFilesBuilder withRotationThreshold( long rotationThreshold )
    {
        this.rotationThreshold = rotationThreshold;
        return this;
    }

    public LogFilesBuilder withDependencies( Dependencies dependencies )
    {
        this.dependencies = dependencies;
        return this;
    }

    public LogFiles build() throws IOException
    {
        TransactionLogFilesContext filesContext = buildContext();
        File logsDirectory = getLogsDirectory();
        filesContext.getFileSystem().mkdirs( logsDirectory );
        return new TransactionLogFiles( logsDirectory, logFileName, filesContext );
    }

    private File getLogsDirectory()
    {
        if ( logsDirectory != null )
        {
            return logsDirectory;
        }
        if ( config != null )
        {
            File neo4jHome = config.get( GraphDatabaseSettings.neo4j_home );
            File databasePath = config.get( database_path );
            File logicalLogsLocation = config.get( GraphDatabaseSettings.logical_logs_location );
            if ( storeDirectory.equals( neo4jHome ) && databasePath.equals( logicalLogsLocation ) )
            {
                return storeDirectory;
            }
            if ( logicalLogsLocation.isAbsolute() )
            {
                return logicalLogsLocation;
            }
            if ( neo4jHome == null || !storeDirectory.equals( databasePath ) )
            {
                Path relativeLogicalLogPath = databasePath.toPath().relativize( logicalLogsLocation.toPath() );
                return new File( storeDirectory, relativeLogicalLogPath.toString() );
            }
            return logicalLogsLocation;
        }
        return storeDirectory;
    }

    TransactionLogFilesContext buildContext() throws IOException
    {
        if ( logEntryReader == null )
        {
            logEntryReader = new VersionAwareLogEntryReader();
        }
        if ( logFileCreationMonitor == null )
        {
            logFileCreationMonitor = LogFileCreationMonitor.NO_MONITOR;
        }
        requireNonNull( fileSystem );
        Supplier<LogVersionRepository> logVersionRepositorySupplier = getLogVersionRepositorySupplier();
        LongSupplier lastCommittedIdSupplier = lastCommittedIdSupplier();
        LongSupplier committingTransactionIdSupplier = committingIdSupplier();

        // Register listener for rotation threshold
        AtomicLong rotationThreshold = getRotationThresholdAndRegisterForUpdates();

        return new TransactionLogFilesContext( rotationThreshold, logEntryReader,
                lastCommittedIdSupplier, committingTransactionIdSupplier, logFileCreationMonitor, logVersionRepositorySupplier, fileSystem );
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
        if ( config == null )
        {
            config = Config.defaults();
        }
        AtomicLong configThreshold = new AtomicLong( config.get( logical_log_rotation_threshold ) );
        config.registerDynamicUpdateListener( logical_log_rotation_threshold, ( prev, update ) -> configThreshold.set( update ) );
        return configThreshold;
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
                    "operation that require availability of log version repository. Please build full version of log " +
                    "files. Please build full version of log files to be able to use them." );
            };
        }
        if ( readOnly )
        {
            requireNonNull( pageCache, "Read only log files require page cache to be able to read current log version." );
            requireNonNull( storeDirectory,"Store directory is required.");
            ReadOnlyLogVersionRepository logVersionRepository =
                    new ReadOnlyLogVersionRepository( pageCache, storeDirectory );
            return () -> logVersionRepository;
        }
        else
        {
            requireNonNull( dependencies, LogVersionRepository.class.getSimpleName() + " is required. " +
                    "Please provide an instance or a dependencies where it can be found." );
            return getSupplier( LogVersionRepository.class );
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
            requireNonNull( pageCache, "Read only log files require page cache to be able to read commited " +
                    "transaction info from store store." );
            requireNonNull( storeDirectory, "Store directory is required." );
            ReadOnlyTransactionIdStore transactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDirectory );
            return transactionIdStore::getLastCommittedTransactionId;
        }
        else
        {
            requireNonNull( dependencies, TransactionIdStore.class.getSimpleName() + " is required. " +
                    "Please provide an instance or a dependencies where it can be found." );
            return () -> resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
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
            requireNonNull( pageCache, "Read only log files require page cache to be able to read commited " +
                    "transaction info from store store." );
            requireNonNull( storeDirectory, "Store directory is required." );
            ReadOnlyTransactionIdStore transactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDirectory );
            return transactionIdStore::committingTransactionId;
        }
        else
        {
            requireNonNull( dependencies, TransactionIdStore.class.getSimpleName() + " is required. " +
                    "Please provide an instance or a dependencies where it can be found." );
            return () -> resolveDependency( TransactionIdStore.class ).committingTransactionId();
        }
    }

    private <T> Supplier<T> getSupplier( Class<T> clazz )
    {
        return () -> resolveDependency( clazz );
    }

    private <T> T resolveDependency( Class<T> clazz )
    {
        return dependencies.resolveDependency( clazz );
    }
}
