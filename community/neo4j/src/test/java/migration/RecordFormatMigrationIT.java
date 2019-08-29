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
package migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.internal.helpers.Exceptions.rootCause;

@TestDirectoryExtension
class RecordFormatMigrationIT
{
    @Inject
    private TestDirectory testDirectory;
    private File databaseDirectory;

    @BeforeEach
    void setUp()
    {
        databaseDirectory = testDirectory.storeDir();
    }

    @Test
    void failToDowngradeFormatWhenUpgradeNotAllowed()
    {
        DatabaseManagementService managementService = startManagementService( StandardV3_4.NAME );
        GraphDatabaseService database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();
        managementService = startManagementService( StandardV4_0.NAME );
        GraphDatabaseService databaseService = getDefaultDatabase( managementService );
        try
        {
            DatabaseContext databaseContext = assertDefaultDatabaseFailed( databaseService );
            Throwable throwable = databaseContext.failureCause();
            assertSame( UpgradeNotAllowedException.class, rootCause( throwable ).getClass() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void failToDowngradeFormatWheUpgradeAllowed()
    {
        DatabaseManagementService managementService = startManagementService( StandardV4_0.NAME );
        GraphDatabaseService database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startDatabaseServiceWithUpgrade( databaseDirectory, StandardV3_4.NAME );
        GraphDatabaseService databaseService = getDefaultDatabase( managementService );
        try
        {
            DatabaseContext databaseContext = assertDefaultDatabaseFailed( databaseService );
            Throwable throwable = databaseContext.failureCause();
            assertSame( StoreUpgrader.AttemptedDowngradeException.class, rootCause( throwable ).getClass() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void skipMigrationIfFormatSpecifiedInConfig()
    {
        DatabaseManagementService managementService = startManagementService( StandardV3_4.NAME );
        GraphDatabaseService database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startManagementService( StandardV3_4.NAME );
        GraphDatabaseAPI nonUpgradedStore = (GraphDatabaseAPI) getDefaultDatabase( managementService );
        RecordStorageEngine storageEngine = nonUpgradedStore.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        assertEquals( StandardV3_4.NAME, storageEngine.testAccessNeoStores().getRecordFormats().name() );
        managementService.shutdown();
    }

    @Test
    void requestMigrationIfStoreFormatNotSpecifiedButIsAvailableInRuntime()
    {
        DatabaseManagementService managementService = startManagementService( StandardV3_4.NAME );
        GraphDatabaseService database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = new DatabaseManagementServiceBuilder( databaseDirectory ).build();
        GraphDatabaseService databaseService = getDefaultDatabase( managementService );
        try
        {
            assertDefaultDatabaseFailed( databaseService );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void latestRecordNotMigratedWhenFormatBumped()
    {
        DatabaseManagementService managementService = startManagementService( StandardV3_4.NAME );
        GraphDatabaseService database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startManagementService( Standard.LATEST_NAME );
        GraphDatabaseService databaseService = getDefaultDatabase( managementService );
        try
        {
            DatabaseContext databaseContext = assertDefaultDatabaseFailed( databaseService );
            Throwable exception = databaseContext.failureCause();
            assertSame( UpgradeNotAllowedException.class, rootCause( exception ).getClass() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private DatabaseContext assertDefaultDatabaseFailed( GraphDatabaseService databaseService )
    {
        assertThrows( Throwable.class, databaseService::beginTx );
        DatabaseManager<?> databaseManager = getDatabaseManager( (GraphDatabaseAPI) databaseService );
        DatabaseContext databaseContext = databaseManager.getDatabaseContext( testDirectory.databaseLayout().getDatabaseName() ).get();
        assertTrue( databaseContext.isFailed() );
        return databaseContext;
    }

    private static DatabaseManager<?> getDatabaseManager( GraphDatabaseAPI databaseService )
    {
        return databaseService.getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private static GraphDatabaseService getDefaultDatabase( DatabaseManagementService managementService )
    {
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private DatabaseManagementService startManagementService( String name )
    {
        return new DatabaseManagementServiceBuilder( databaseDirectory ).setConfig( record_format, name ).build();
    }

    private static DatabaseManagementService startDatabaseServiceWithUpgrade( File storeDir, String formatName )
    {
        return new DatabaseManagementServiceBuilder( storeDir ).setConfig( record_format, formatName )
                .setConfig( allow_upgrade, true ).build();
    }
}
