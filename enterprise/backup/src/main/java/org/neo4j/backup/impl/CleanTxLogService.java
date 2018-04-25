/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.com.storecopy.ExternallyManagedPageCache;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

class CleanTxLogService
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystemAbstraction;

    CleanTxLogService( PageCache pageCache, FileSystemAbstraction fileSystemAbstraction )
    {
        this.pageCache = pageCache;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    public void removeUnnecessaryTransactionLogs( File backupLocation )
    {
        LogFiles logFiles = getLogFiles( backupLocation );
        LongStream.rangeClosed( logFiles.getLowestLogVersion(), logFiles.getHighestLogVersion() )
                .mapToObj( logFiles::getLogFileForVersion )
                .filter( File::exists )
                .forEach( File::delete );
    }

    private LogFiles getLogFiles( File backupDirectory )
    {
        try
        {
            return LogFilesBuilder.activeFilesBuilder( backupDirectory, fileSystemAbstraction, pageCache ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
