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

class ForceCheckpointBackupService implements Lifecycle
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Config config;
    private final File backupDirectory;

    private GraphDatabaseAPI graphDatabaseAPI;

    ForceCheckpointBackupService( PageCache pageCache, FileSystemAbstraction fileSystemAbstraction, Config config, File backupDirectory )
    {
        this.pageCache = pageCache;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.config = config;
        this.backupDirectory = backupDirectory;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        Map<String,String> configParams = config.getRaw();
        configParams.put( GraphDatabaseSettings.logical_logs_location.name(), backupDirectory.toString() );
        configParams.put( GraphDatabaseSettings.pagecache_warmup_enabled.name(), Settings.FALSE );
        configParams.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        configParams.put( GraphDatabaseSettings.keep_logical_logs.name(), Settings.FALSE );
        GraphDatabaseFactory factory = ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );
        graphDatabaseAPI = (GraphDatabaseAPI) factory.newEmbeddedDatabaseBuilder( backupDirectory ).setConfig( configParams ).newGraphDatabase();
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
        graphDatabaseAPI.shutdown();
    }

    void forceCheckpoint()
    {
        CheckPointer checkPointer = graphDatabaseAPI.getDependencyResolver().resolveDependency( CheckPointer.class );
        try
        {
            checkPointer.forceCheckPoint( new SimpleTriggerInfo( "postSuccessfulBackup" ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    void forcePrune()
    {
        LogFiles logFiles = getLogFiles( backupDirectory, pageCache, fileSystemAbstraction );
        LogPruning logPruning = graphDatabaseAPI.getDependencyResolver().resolveDependency( LogPruning.class );
        logPruning.pruneLogs( logFiles.getHighestLogVersion() );
    }

    void forceRotation()
    {
        LogRotation logRotation = graphDatabaseAPI.getDependencyResolver().resolveDependency( LogRotation.class );
        try
        {
            logRotation.rotateLogFile();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private LogFiles getLogFiles( File backupDirectory, PageCache pageCache, FileSystemAbstraction fileSystemAbstraction )
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
