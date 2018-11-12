/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Optional.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
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
        DatabaseLayout layout = testDirectory.databaseLayout( () -> of( txDirectory ) );
        performTransactions( txDirectory.getAbsolutePath(), layout.databaseDirectory() );
        verifyTransactionLogs( layout.getTransactionLogsDirectory(), layout.databaseDirectory() );
    }

    private static void performTransactions( String txPath, File storeDir )
    {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.transaction_logs_root_path, txPath )
                .newGraphDatabase();
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = database.createNode();
                node.setProperty( "a", "b" );
                node.setProperty( "c", "d" );
                transaction.success();
            }
        }
        database.shutdown();
    }

    private void verifyTransactionLogs( File txDirectory, File storeDir ) throws IOException
    {
        LogFiles storeDirLogs = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDir, fileSystem ).build();
        assertFalse( storeDirLogs.versionExists( 0 ) );

        LogFiles txDirectoryLogs = LogFilesBuilder.logFilesBasedOnlyBuilder( txDirectory, fileSystem ).build();
        assertTrue( txDirectoryLogs.versionExists( 0 ) );
        try ( PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel = txDirectoryLogs.openForVersion( 0 ) )
        {
            assertThat( physicalLogVersionedStoreChannel.size(), greaterThan( 0L ) );
        }
    }

}
