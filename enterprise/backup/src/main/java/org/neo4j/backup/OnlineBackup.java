/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.neo4j.backup.impl.BackupClient;
import org.neo4j.backup.impl.BackupOutcome;
import org.neo4j.backup.impl.BackupProtocolService;
import org.neo4j.backup.impl.BackupServer;
import org.neo4j.backup.impl.ConsistencyCheck;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * This class encapsulates the information needed to perform an online backup against a running Neo4j instance
 * configured to act as a backup server. This class is not instantiable, instead factory methods are used to get
 * instances configured to contact a specific backup server against which all possible backup operations can be
 * performed.
 *
 * All backup methods return the same instance, allowing for chaining calls.
 */
public class OnlineBackup
{
    private final String hostNameOrIp;
    private final int port;
    private boolean forensics;
    private BackupOutcome outcome;
    private long timeoutMillis = BackupClient.BIG_READ_TIMEOUT;
    private OutputStream out = System.out;

    /**
     * Factory method for this class. The OnlineBackup instance returned will perform backup operations against the
     * hostname and port passed in as parameters.
     *
     * @param hostNameOrIp The hostname or the IP address of the backup server
     * @param port The port at which the remote backup server is listening
     * @return An OnlineBackup instance ready to perform backup operations from the given remote server
     */
    public static OnlineBackup from( String hostNameOrIp, int port )
    {
        return new OnlineBackup( hostNameOrIp, port );
    }

    /**
     * Factory method for this class. The OnlineBackup instance returned will perform backup operations against the
     * hostname passed in as parameter, using the default backup port.
     *
     * @param hostNameOrIp The hostname or IP address of the backup server
     * @return An OnlineBackup instance ready to perform backup operations from the given remote server
     */
    public static OnlineBackup from( String hostNameOrIp )
    {
        return new OnlineBackup( hostNameOrIp, BackupServer.DEFAULT_PORT );
    }

    private OnlineBackup( String hostNameOrIp, int port )
    {
        this.hostNameOrIp = hostNameOrIp;
        this.port = port;
    }

    /**
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated use {@link #backup(File)} instead
     */
    @Deprecated
    public OnlineBackup backup( String targetDirectory )
    {
        return backup( new File( targetDirectory ) );
    }

    /**
     * Performs a backup into targetDirectory. The server contacted is the one configured in the factory method used to
     * obtain this instance. After the backup is complete, a verification phase will take place, checking
     * the database for consistency. If any errors are found, they will be printed in stderr.
     *
     * If the target directory does not contain a database, a full backup will be performed, otherwise an incremental
     * backup mechanism is used.
     *
     * If the backup has become too far out of date for an incremental backup to succeed, a full backup is performed.
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated use {@link #backup(File, Config, boolean, String)} instead.
     */
    @Deprecated
    public OnlineBackup backup( File targetDirectory )
    {
        Path dir = targetDirectory.toPath();
        Config config = defaultConfig();
        outcome = new BackupProtocolService( out, config ).doIncrementalBackupOrFallbackToFull(
                hostNameOrIp, port, dir, getConsistencyCheck( true ), config, timeoutMillis, forensics );
        return this;
    }

    /**
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param verification If true, the verification phase will be run.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated use {@link #backup(File, boolean)} instead
     */
    @Deprecated
    public OnlineBackup backup( String targetDirectory, boolean verification )
    {
        return backup( new File( targetDirectory ), verification );
    }

    /**
     * Performs a backup into targetDirectory. The server contacted is the one configured in the factory method used to
     * obtain this instance. After the backup is complete, and if the verification parameter is set to true,
     * a verification phase will take place, checking the database for consistency. If any errors are found, they will
     * be printed in stderr.
     *
     * If the target directory does not contain a database, a full backup will be performed, otherwise an incremental
     * backup mechanism is used.
     *
     * If the backup has become too far out of date for an incremental backup to succeed, a full backup is performed.
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param verification If true, the verification phase will be run.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated use {@link #backup(File, Config, boolean, String)} instead.
     */
    @Deprecated
    public OnlineBackup backup( File targetDirectory, boolean verification )
    {
        Config config = defaultConfig();
        outcome = new BackupProtocolService( out, config ).doIncrementalBackupOrFallbackToFull(
                hostNameOrIp, port, targetDirectory.toPath(), getConsistencyCheck( verification ), config,
                timeoutMillis, forensics );
        return this;
    }

    /**
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param tuningConfiguration The {@link Config} to use when running the consistency check
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated use {@link #backup(File, Config, boolean, String)} instead
     */
    @Deprecated
    public OnlineBackup backup( String targetDirectory, Config tuningConfiguration )
    {
        return backup( new File( targetDirectory ), tuningConfiguration );
    }

    /**
     * Performs a backup into targetDirectory. The server contacted is the one configured in the factory method used to
     * obtain this instance. After the backup is complete, a verification phase will take place, checking
     * the database for consistency. If any errors are found, they will be printed in stderr.
     *
     * If the target directory does not contain a database, a full backup will be performed, otherwise an incremental
     * backup mechanism is used.
     *
     * If the backup has become too far out of date for an incremental backup to succeed, a full backup is performed.
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param tuningConfiguration The {@link Config} to use when running the consistency check
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated use {@link #backup(File, Config, boolean, String)} instead.
     */
    @Deprecated
    public OnlineBackup backup( File targetDirectory, Config tuningConfiguration )
    {
        outcome = new BackupProtocolService( out, defaultConfig() ).doIncrementalBackupOrFallbackToFull(
                hostNameOrIp, port, targetDirectory.toPath(), getConsistencyCheck( true ), tuningConfiguration,
                timeoutMillis, forensics );
        return this;
    }

    /**
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param tuningConfiguration The {@link Config} to use when running the consistency check
     * @param verification If true, the verification phase will be run.
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     * @deprecated use {@link #backup(File, Config, boolean, String)} instead
     */
    @Deprecated
    public OnlineBackup backup( String targetDirectory, Config tuningConfiguration, boolean verification )
    {
        return backup( new File( targetDirectory ), tuningConfiguration, verification );
    }

    /**
     * Performs a backup into targetDirectory. The server contacted is the one configured in the factory method used to
     * obtain this instance. After the backup is complete, and if the verification parameter is set to true,
     * a verification phase will take place, checking the database for consistency. If any errors are found, they will
     * be printed in stderr.
     *
     * If the target directory does not contain a database, a full backup will be performed, otherwise an incremental
     * backup mechanism is used.
     *
     * If the backup has become too far out of date for an incremental backup to succeed, a full backup is performed.
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param tuningConfiguration The {@link Config} to use when running the consistency check
     * @param verification If true, the verification phase will be run.
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     * @deprecated use {@link #backup(File, Config, boolean, String)} instead.
     */
    @Deprecated
    public OnlineBackup backup( File targetDirectory, Config tuningConfiguration, boolean verification )
    {
        outcome = new BackupProtocolService( out, defaultConfig() ).doIncrementalBackupOrFallbackToFull(
                hostNameOrIp, port, targetDirectory.toPath(), getConsistencyCheck( verification ), tuningConfiguration,
                timeoutMillis, forensics );
        return this;
    }

    /**
     * Performs a backup into targetDirectory. The server contacted is the one configured in the factory method used to
     * obtain this instance. After the backup is complete, and if the verification parameter is set to true,
     * a verification phase will take place, checking the database for consistency. If any errors are found, they will
     * be printed in stderr.
     *
     * If the target directory does not contain a database, a full backup will be performed, otherwise an incremental
     * backup mechanism is used.
     *
     * If the backup has become too far out of date for an incremental backup to succeed, a full backup is performed.
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param tuningConfiguration The {@link Config} to use when running the consistency check
     * @param verification If true, the verification phase will be run.
     * @param pageCacheSize The size of the page cache used for the backup process, e.g. "2 GiB".
     * This value is parsed in the same way as the {@link GraphDatabaseSettings#pagecache_memory} setting.
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     */
    public OnlineBackup backup( File targetDirectory, Config tuningConfiguration, boolean verification, String pageCacheSize )
    {
        Objects.requireNonNull( targetDirectory, "The 'targetDirectory' parameter cannot be null." );
        Objects.requireNonNull( pageCacheSize, "The 'pageCacheSize' parameter cannot be null." );

        Config config = Config.defaults( GraphDatabaseSettings.pagecache_memory, pageCacheSize );
        if ( tuningConfiguration == null )
        {
            tuningConfiguration = config;
        }
        else if ( !tuningConfiguration.isConfigured( GraphDatabaseSettings.pagecache_memory ) )
        {
            tuningConfiguration = Config.fromSettings( tuningConfiguration.getRaw() )
                    .withSetting( GraphDatabaseSettings.pagecache_memory, pageCacheSize )
                    .build();
        }
        outcome = new BackupProtocolService( out, config ).doIncrementalBackupOrFallbackToFull(
                hostNameOrIp, port, targetDirectory.toPath(), getConsistencyCheck( verification ), tuningConfiguration, timeoutMillis, forensics );
        return this;
    }

    /**
     * Use this method to change the default timeout to keep the client waiting for each reply from the server when
     * doing online backup. Once the value is changed, then every time when doing online backup, the timeout will be
     * reused until this method is called again and a new value is assigned.
     *
     * @param timeoutMillis The time duration in millisecond that keeps the client waiting for each reply from the
     * server.
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     */
    public OnlineBackup withTimeout( long timeoutMillis )
    {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    public OnlineBackup withOutput( OutputStream out )
    {
        this.out = out;
        return this;
    }

    /**
     * Performs a full backup storing the resulting database at the given directory. The server contacted is the one
     * configured in the factory method used to obtain this instance. At the end of the backup, a verification phase
     * will take place, running over the resulting database ensuring it is consistent. If the check fails, the fact
     * will be printed in stderr.
     *
     * If the target directory already contains a database, a RuntimeException denoting the fact will be thrown.
     *
     * @param targetDirectory The directory in which to store the database
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     * @deprecated Use {@link #backup(File)} instead.
     */
    @Deprecated
    public OnlineBackup full( String targetDirectory )
    {
        Config config = defaultConfig();
        outcome = new BackupProtocolService( out, config ).doFullBackup( hostNameOrIp, port, Paths.get( targetDirectory ),
                getConsistencyCheck( true ), config, timeoutMillis, forensics );
        return this;
    }

    /**
     * Performs a full backup storing the resulting database at the given directory. The server contacted is the one
     * configured in the factory method used to obtain this instance. If the verification flag is set, at the end of
     * the backup, a verification phase will take place, running over the resulting database ensuring it is consistent.
     * If the check fails, the fact will be printed in stderr.
     *
     * If the target directory already contains a database, a RuntimeException denoting the fact will be thrown.
     *
     * @param targetDirectory The directory in which to store the database
     * @param verification a boolean indicating whether to perform verification on the created backup
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     * @deprecated Use {@link #backup(File, boolean)} instead
     */
    @Deprecated
    public OnlineBackup full( String targetDirectory, boolean verification )
    {
        Config config = defaultConfig();
        outcome = new BackupProtocolService( out, config ).doFullBackup( hostNameOrIp, port, Paths.get( targetDirectory ),
                getConsistencyCheck( verification ), config, timeoutMillis, forensics );
        return this;
    }

    /**
     * Performs a full backup storing the resulting database at the given directory. The server contacted is the one
     * configured in the factory method used to obtain this instance. If the verification flag is set, at the end of
     * the backup, a verification phase will take place, running over the resulting database ensuring it is consistent.
     * If the check fails, the fact will be printed in stderr. The consistency check will run with the provided
     * tuning configuration.
     *
     * If the target directory already contains a database, a RuntimeException denoting the fact will be thrown.
     *
     * @param targetDirectory The directory in which to store the database
     * @param verification a boolean indicating whether to perform verification on the created backup
     * @param tuningConfiguration The {@link Config} to use when running the consistency check
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     * @deprecated Use {@link #backup(File, Config, boolean)} instead.
     */
    @Deprecated
    public OnlineBackup full( String targetDirectory, boolean verification, Config tuningConfiguration )
    {
        outcome = new BackupProtocolService( out, tuningConfiguration ).doFullBackup( hostNameOrIp, port, Paths.get( targetDirectory ),
                getConsistencyCheck( verification ), tuningConfiguration, timeoutMillis, forensics );
        return this;
    }

    /**
     * Performs an incremental backup on the database stored in targetDirectory. The server contacted is the one
     * configured in the factory method used to obtain this instance. After the incremental backup is complete, a
     * verification phase will take place, checking the database for consistency. If any errors are found, they will
     * be printed in stderr.
     *
     * If the target directory does not contain a database or it is not compatible with the one present in the
     * configured backup server a RuntimeException will be thrown denoting the fact.
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated Use {@link #backup(File)} instead.
     */
    @Deprecated
    public OnlineBackup incremental( String targetDirectory )
    {
        Config config = defaultConfig();
        outcome = new BackupProtocolService( out, config ).doIncrementalBackup(
                hostNameOrIp, port, Paths.get( targetDirectory ), getConsistencyCheck( false ), timeoutMillis, config );
        return this;
    }

    /**
     * Performs an incremental backup on the database stored in targetDirectory. The server contacted is the one
     * configured in the factory method used to obtain this instance. After the incremental backup is complete, and if
     * the verification parameter is set to true, a  verification phase will take place, checking the database for
     * consistency. If any errors are found, they will be printed in stderr.
     *
     * If the target directory does not contain a database or it is not compatible with the one present in the
     * configured backup server a RuntimeException will be thrown denoting the fact.
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param verification If true, the verification phase will be run.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     * @deprecated Use {@link #backup(File, boolean)} instead.
     */
    @Deprecated
    public OnlineBackup incremental( String targetDirectory, boolean verification )
    {
        Config config = defaultConfig();
        outcome = new BackupProtocolService( out, config ).doIncrementalBackup(
                hostNameOrIp, port, Paths.get( targetDirectory ), getConsistencyCheck( verification ), timeoutMillis, config );
        return this;
    }

    /**
     * Performs an incremental backup on the supplied target database. The server contacted is the one
     * configured in the factory method used to obtain this instance. After the incremental backup is complete
     * a verification phase will take place, checking the database for consistency. If any errors are found, they will
     * be printed in stderr.
     *
     * If the target database is not compatible with the one present in the target backup server, a RuntimeException
     * will be thrown denoting the fact.
     *
     * @param targetDb The database on which the incremental backup is to be applied
     * @return The same OnlineBackup instance, possible to use for a new backup operation.
     * @deprecated Use {@link #backup(String)} instead.
     */
    @Deprecated
    public OnlineBackup incremental( GraphDatabaseAPI targetDb )
    {
        outcome = new BackupProtocolService( out, defaultConfig() ).doIncrementalBackup( hostNameOrIp, port, targetDb, timeoutMillis );
        return this;
    }

    /**
     * Provides information about the last committed transaction for each data source present in the last backup
     * operation performed by this OnlineBackup.
     * In particular, it returns a map where the keys are the names of the data sources and the values the longs that
     * are the last committed transaction id for that data source.
     *
     * @return A map from data source name to last committed transaction id.
     */
    public long getLastCommittedTx()
    {
        return outcome().getLastCommittedTx();
    }

    /**
     * @return the consistency outcome of the last made backup. I
     */
    public boolean isConsistent()
    {
        return outcome().isConsistent();
    }

    private BackupOutcome outcome()
    {
        if ( outcome == null )
        {
            throw new IllegalStateException( "No outcome yet. Please call full or incremental backup first" );
        }
        return outcome;
    }

    private Config defaultConfig()
    {
        return Config.defaults();
    }

    /**
     * @param forensics whether or not additional information should be backed up, for example transaction
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     */
    public OnlineBackup gatheringForensics( boolean forensics )
    {
        this.forensics = forensics;
        return this;
    }

    private static ConsistencyCheck getConsistencyCheck( boolean verification )
    {
        return verification ? ConsistencyCheck.FULL : ConsistencyCheck.NONE;
    }
}
