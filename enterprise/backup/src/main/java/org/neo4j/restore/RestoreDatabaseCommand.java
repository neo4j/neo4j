/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.restore;

import java.io.File;
import java.io.IOException;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.Validators;

import static java.lang.String.format;
import static org.neo4j.commandline.Util.checkLock;
import static org.neo4j.commandline.Util.isSameOrChildFile;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;

public class RestoreDatabaseCommand
{
    private FileSystemAbstraction fs;
    private final File fromDatabasePath;
    private final File toDatabaseDir;
    private final File transactionLogsDirectory;
    private String toDatabaseName;
    private boolean forceOverwrite;

    public RestoreDatabaseCommand( FileSystemAbstraction fs, File fromDatabasePath, Config config, String toDatabaseName,
            boolean forceOverwrite )
    {
        this.fs = fs;
        this.fromDatabasePath = fromDatabasePath;
        this.forceOverwrite = forceOverwrite;
        this.toDatabaseName = toDatabaseName;
        this.toDatabaseDir = config.get( database_path ).getAbsoluteFile();
        this.transactionLogsDirectory = config.get( GraphDatabaseSettings.logical_logs_location ).getAbsoluteFile();
    }

    public void execute() throws IOException, CommandFailed
    {
        if ( !fs.fileExists( fromDatabasePath ) )
        {
            throw new IllegalArgumentException( format( "Source directory does not exist [%s]", fromDatabasePath ) );
        }

        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( fromDatabasePath );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException(
                    format( "Source directory is not a database backup [%s]", fromDatabasePath ) );
        }

        if ( fs.fileExists( toDatabaseDir ) && !forceOverwrite )
        {
            throw new IllegalArgumentException( format( "Database with name [%s] already exists at %s", toDatabaseName, toDatabaseDir ) );
        }

        checkLock( DatabaseLayout.of( toDatabaseDir ).getStoreLayout() );

        fs.deleteRecursively( toDatabaseDir );

        if ( !isSameOrChildFile( toDatabaseDir, transactionLogsDirectory ) )
        {
            fs.deleteRecursively( transactionLogsDirectory );
        }
        LogFiles backupLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( fromDatabasePath, fs ).build();
        restoreDatabaseFiles( backupLogFiles, fromDatabasePath.listFiles() );
    }

    private void restoreDatabaseFiles( LogFiles backupLogFiles, File[] files ) throws IOException
    {
        if ( files != null )
        {
            for ( File file : files )
            {
                if ( file.isDirectory() )
                {
                    File destination = new File( toDatabaseDir, file.getName() );
                    fs.mkdirs( destination );
                    fs.copyRecursively( file, destination );
                }
                else
                {
                    fs.copyToDirectory( file, backupLogFiles.isLogFile( file ) ? transactionLogsDirectory : toDatabaseDir );
                }
            }
        }
    }
}
