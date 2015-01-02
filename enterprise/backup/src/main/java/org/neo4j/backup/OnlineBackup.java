/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.util.Map;

import org.neo4j.backup.BackupService.BackupOutcome;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

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
    private BackupService.BackupOutcome outcome;

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
     * Performs a backup into targetDirectory. The server contacted is the one configured in the factory method used to
     * obtain this instance. After the backup is complete, a verification phase will take place, checking
     * the database for consistency. If any errors are found, they will be printed in stderr.
     *
     * If the target directory does not contain a database, a full backup will be performed, otherwise an incremental
     * backup mechanism is used.
     *
     * If the backup has become too far out of date for an incremental backup to succeed, a full backup is performed.
     *
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     */
    public OnlineBackup backup( String targetDirectory )
    {
        outcome = new BackupService().doIncrementalBackupOrFallbackToFull( hostNameOrIp, port, targetDirectory, true,
                defaultConfig() );
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
     *
     * @param targetDirectory A directory holding a complete database previously obtained from the backup server.
     * @param verification If true, the verification phase will be run.
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     */
    public OnlineBackup backup( String targetDirectory, boolean verification )
    {
        outcome = new BackupService().doIncrementalBackupOrFallbackToFull( hostNameOrIp, port, targetDirectory,
                verification, defaultConfig() );
        return this;
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
     */
    public OnlineBackup backup( String targetDirectory, Config tuningConfiguration )
    {
        outcome = new BackupService().doIncrementalBackupOrFallbackToFull( hostNameOrIp, port, targetDirectory, true,
                tuningConfiguration );
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
     * @return The same OnlineBackup instance, possible to use for a new backup operation
     */
    public OnlineBackup backup( String targetDirectory, Config tuningConfiguration,
                                boolean verification )
    {
        outcome = new BackupService().doIncrementalBackupOrFallbackToFull( hostNameOrIp, port, targetDirectory,
                verification, tuningConfiguration );
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
     * @deprecated Use {@link #backup(String)} instead.
     */
    @Deprecated
    public OnlineBackup full( String targetDirectory )
    {
        outcome = new BackupService().doFullBackup( hostNameOrIp, port, targetDirectory, true, defaultConfig() );
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
     * @deprecated Use {@link #backup(String, boolean)} instead
     */
    @Deprecated
    public OnlineBackup full( String targetDirectory, boolean verification )
    {
        outcome = new BackupService().doFullBackup( hostNameOrIp, port, targetDirectory, verification,
                defaultConfig() );
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
     * @deprecated Use {@link #backup(String, Config, boolean)} instead.
     */
    @Deprecated
    public OnlineBackup full( String targetDirectory, boolean verification, Config tuningConfiguration )
    {
        outcome = new BackupService().doFullBackup( hostNameOrIp, port, targetDirectory, verification,
                tuningConfiguration );
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
     * @deprecated Use {@link #backup(String)} instead.
     */
    @Deprecated
    public OnlineBackup incremental( String targetDirectory )
    {
        outcome = new BackupService().doIncrementalBackup( hostNameOrIp, port, targetDirectory, true );
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
     * @deprecated Use {@link #backup(String, boolean)} instead.
     */
    @Deprecated
    public OnlineBackup incremental( String targetDirectory, boolean verification )
    {
        outcome = new BackupService().doIncrementalBackup( hostNameOrIp, port, targetDirectory, verification );
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
        outcome = new BackupService().doIncrementalBackup( hostNameOrIp, port, targetDb );
        return this;
    }

    /**
     * Provides information about the last committed transaction for each data source present in the last backup
     * operation performed by this OnlineBackup.
     * In particular, it returns a map where the keys are the names of the data sources and the values the longs that
     * are the last committed transaction id for that data source.
     * @return A map from data source name to last committed transaction id.
     */
    public Map<String, Long> getLastCommittedTxs()
    {
        return outcome().getLastCommittedTxs();
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
        return new Config( stringMap(), GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }
}
