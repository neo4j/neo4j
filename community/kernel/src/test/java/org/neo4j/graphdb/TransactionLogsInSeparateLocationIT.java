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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TransactionLogsInSeparateLocationIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final FileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void databaseWithTransactionLogsInSeparateRelativeLocation() throws IOException
    {
        File storeDir = testDirectory.graphDbDir();
        File txDirectory = new File( storeDir, "transaction-logs" );
        performTransactions( txDirectory.getName(), storeDir );
        verifyTransactionLogs( txDirectory, storeDir );
    }

    @Test
    public void databaseWithTransactionLogsInSeparateAbsoluteLocation() throws IOException
    {
        File storeDir = testDirectory.graphDbDir();
        File txDirectory = testDirectory.directory( "transaction-logs" );
        performTransactions( txDirectory.getAbsolutePath(), storeDir );
        verifyTransactionLogs( txDirectory, storeDir );
    }

    private void performTransactions( String txPath, File storeDir )
    {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.logical_logs_location, txPath )
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
        FileSystemAbstraction fileSystem = fileSystemRule.get();
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
