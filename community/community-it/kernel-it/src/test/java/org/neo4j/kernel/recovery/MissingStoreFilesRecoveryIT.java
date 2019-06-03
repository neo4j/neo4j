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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.configuration.Settings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_missing_files;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class MissingStoreFilesRecoveryIT
{
    private final DatabaseId databaseId = new DatabaseId( DEFAULT_DATABASE_NAME );

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    private DatabaseManagementService managementService;
    private DatabaseLayout databaseLayout;
    private DatabaseManagementServiceBuilder serviceBuilder;

    @BeforeEach
    void setUp()
    {
        serviceBuilder = new DatabaseManagementServiceBuilder( testDirectory.directory() );
        managementService = serviceBuilder.build();
        var databaseApi = defaultDatabase( managementService );
        databaseLayout = databaseApi.databaseLayout();
        managementService.shutdown();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void databaseStartFailingOnMissingFilesAndMissedTxLogs() throws IOException
    {
        fileSystem.deleteFile( databaseLayout.nodeStore() );
        fileSystem.deleteRecursively( databaseLayout.getTransactionLogsDirectory() );

        managementService = serviceBuilder.build();
        DatabaseManager<?> databaseManager = getDatabaseManager();
        var databaseContext = databaseManager.getDatabaseContext( databaseId ).get();
        assertTrue( databaseContext.isFailed() );
    }

    @Test
    void databaseStartForcedOnMissingFilesAndMissedTxLogs() throws IOException
    {
        fileSystem.deleteFile( databaseLayout.nodeStore() );
        fileSystem.deleteRecursively( databaseLayout.getTransactionLogsDirectory() );

        managementService = serviceBuilder.setConfig( fail_on_missing_files, Settings.FALSE ).build();
        DatabaseManager<?> databaseManager = getDatabaseManager();
        var databaseContext = databaseManager.getDatabaseContext( databaseId ).get();
        assertFalse( databaseContext.isFailed() );
    }

    @Test
    void databaseFilesRestoredAfterRecovery()
    {
        fileSystem.deleteFile( databaseLayout.relationshipGroupStore() );

        DatabaseManager<?> databaseManager = getDatabaseManager();
        var databaseContext = databaseManager.getDatabaseContext( databaseId ).get();
        assertFalse( databaseContext.isFailed() );
        fileSystem.fileExists( databaseLayout.relationshipGroupStore() );
    }

    @Test
    void databaseFilesRestoredOnMissingFilesAndAllTransactionLogs()
    {
        fileSystem.deleteFile( databaseLayout.nodeStore() );

        managementService = serviceBuilder.build();
        DatabaseManager<?> databaseManager = getDatabaseManager();
        var databaseContext = databaseManager.getDatabaseContext( databaseId ).get();
        assertFalse( databaseContext.isFailed() );
    }

    private DatabaseManager getDatabaseManager()
    {
        return defaultDatabase( managementService ).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private GraphDatabaseAPI defaultDatabase( DatabaseManagementService managementService )
    {
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }
}
