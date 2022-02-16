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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;

@PageCacheExtension
@Neo4jLayoutExtension
class LogVersionUpgradeCheckerIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp()
    {
        databaseLayout = DatabaseLayout.ofFlat( testDirectory.directory( DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void startAsNormalWhenUpgradeIsNotAllowed()
    {
        createGraphDbAndKillIt();

        // Try to start with upgrading disabled
        DatabaseManagementService managementService = startDatabaseService( false );
        managementService.database( DEFAULT_DATABASE_NAME );
        managementService.shutdown();
    }

    @Test
    void startFromOlderTransactionLogsIfAllowed() throws Exception
    {
        createStoreWithLogEntryVersion( KernelVersion.EARLIEST.version(), true );

        // Try to start with upgrading enabled
        DatabaseManagementService managementService = startDatabaseService( true );
        managementService.database( DEFAULT_DATABASE_NAME );
        managementService.shutdown();
    }

    private void createGraphDbAndKillIt()
    {
        DatabaseManagementService managementService = startDatabaseService( false );
        final GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label( "FOO" ) );
            tx.createNode( label( "BAR" ) );
            tx.commit();
        }

        managementService.shutdown();
    }

    private void createStoreWithLogEntryVersion( byte logEntryVersion, boolean cleanup ) throws Exception
    {
        createGraphDbAndKillIt();
        appendCheckpoint( logEntryVersion, cleanup );
    }

    private void appendCheckpoint( byte logEntryVersion, boolean removeCheckpointFile ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( databaseLayout, fileSystem, pageCache )
                .withStorageEngineFactory( StorageEngineFactory.defaultStorageEngine() )
                .withStoreId( StoreId.UNKNOWN ).build();

        if ( removeCheckpointFile )
        {
            for ( Path checkpointFile : logFiles.getCheckpointFile().getDetachedCheckpointFiles() )
            {
                fileSystem.deleteFile( checkpointFile );
            }
        }

        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            CheckpointAppender appender = logFiles.getCheckpointFile().getCheckpointAppender();
            appender.checkPoint( LogCheckPointEvent.NULL, UNKNOWN_TRANSACTION_ID, new LogPosition( 0, CURRENT_FORMAT_LOG_HEADER_SIZE ), Instant.now(), "test" );
        }
    }

    private DatabaseManagementService startDatabaseService( boolean allowUpgrade )
    {
        var rootDirectory = databaseLayout.databaseDirectory().getParent();
        return new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( fileSystem ).impermanent()
                .setConfig( transaction_logs_root_path, rootDirectory )
                .setConfig( allow_upgrade, allowUpgrade ).build();
    }
}
