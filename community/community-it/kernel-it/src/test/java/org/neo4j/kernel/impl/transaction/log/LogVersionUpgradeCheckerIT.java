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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;

@PageCacheExtension
@Neo4jLayoutExtension
class LogVersionUpgradeCheckerIT
{
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    @Test
    void startAsNormalWhenUpgradeIsNotAllowed()
    {
        createGraphDbAndKillIt();

        // Try to start with upgrading disabled
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( fileSystem )
                .impermanent()
                .setConfig( allow_upgrade, false )
                .build();
        managementService.database( DEFAULT_DATABASE_NAME );
        managementService.shutdown();
    }

    @Test
    void failToStartFromOlderTransactionLogsIfNotAllowed() throws Exception
    {
        createStoreWithLogEntryVersion( LogEntryVersion.V3_0_10 );

        // Try to start with upgrading disabled
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( fileSystem )
                .impermanent()
                .setConfig( allow_upgrade, false )
                .build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            DatabaseStateService dbStateService = db.getDependencyResolver().resolveDependency( DatabaseStateService.class );

            var failure = dbStateService.causeOfFailure( db.databaseId() );
            assertTrue( failure.isPresent() );
            assertThat( failure.get() ).hasRootCauseInstanceOf( UpgradeNotAllowedException.class );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startFromOlderTransactionLogsIfAllowed() throws Exception
    {
        createStoreWithLogEntryVersion( LogEntryVersion.V3_0_10 );

        // Try to start with upgrading enabled
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( fileSystem )
                .impermanent()
                .setConfig( allow_upgrade, true )
                .build();
        managementService.database( DEFAULT_DATABASE_NAME );
        managementService.shutdown();
    }

    private void createGraphDbAndKillIt()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( fileSystem )
                .impermanent()
                .build();
        final GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label( "FOO" ) );
            tx.createNode( label( "BAR" ) );
            tx.commit();
        }

        managementService.shutdown();
    }

    private void createStoreWithLogEntryVersion( LogEntryVersion logEntryVersion ) throws Exception
    {
        createGraphDbAndKillIt();
        appendCheckpoint( logEntryVersion );
    }

    private void appendCheckpoint( LogEntryVersion logVersion ) throws IOException
    {
        VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader();
        LogFiles logFiles =
                LogFilesBuilder.activeFilesBuilder( databaseLayout, fileSystem, pageCache ).withLogEntryReader( logEntryReader ).build();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        LogTailScanner.LogTailInformation tailInformation = tailScanner.getTailInformation();

        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            FlushablePositionAwareChecksumChannel channel = logFiles.getLogFile().getWriter();

            LogPosition logPosition = tailInformation.lastCheckPoint.getLogPosition();

            // Fake record
            channel.put( logVersion.version() )
                    .put( CHECK_POINT )
                    .putLong( logPosition.getLogVersion() )
                    .putLong( logPosition.getByteOffset() );

            channel.prepareForFlush().flush();
        }
    }
}
