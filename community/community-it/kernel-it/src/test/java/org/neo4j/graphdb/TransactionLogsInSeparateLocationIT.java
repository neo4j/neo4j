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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

@TestDirectoryExtension
class TransactionLogsInSeparateLocationIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void databaseWithTransactionLogsInSeparateAbsoluteLocation() throws IOException
    {
        File txDirectory = testDirectory.directory( "transaction-logs" );
        Config config = Config.newBuilder()
                .set( neo4j_home, testDirectory.homeDir().toPath() )
                .set( transaction_logs_root_path, txDirectory.toPath().toAbsolutePath() )
                .build();
        DatabaseLayout layout = DatabaseLayout.of( config );
        performTransactions( txDirectory.toPath().toAbsolutePath(), layout.databaseDirectory() );
        verifyTransactionLogs( layout.getTransactionLogsDirectory(), layout.databaseDirectory() );
    }

    private static void performTransactions( Path txPath, File storeDir )
    {
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( storeDir )
                        .setConfig( transaction_logs_root_path, txPath )
                        .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = transaction.createNode();
                node.setProperty( "a", "b" );
                node.setProperty( "c", "d" );
                transaction.commit();
            }
        }
        managementService.shutdown();
    }

    private void verifyTransactionLogs( File txDirectory, File storeDir ) throws IOException
    {
        LogFiles storeDirLogs = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDir, fileSystem ).build();
        assertFalse( storeDirLogs.versionExists( 0 ) );

        LogFiles txDirectoryLogs = LogFilesBuilder.logFilesBasedOnlyBuilder( txDirectory, fileSystem ).build();
        assertTrue( txDirectoryLogs.versionExists( 0 ) );
        try ( PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel = txDirectoryLogs.openForVersion( 0 ) )
        {
            assertThat( physicalLogVersionedStoreChannel.size() ).isGreaterThan( 0L );
        }
    }

}
