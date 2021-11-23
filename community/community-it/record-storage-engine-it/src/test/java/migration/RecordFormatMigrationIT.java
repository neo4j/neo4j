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
package migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV5_0;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.storage_engine;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

// Not possible to test until the 5.0 formats are enabled
@Disabled
@TestDirectoryExtension
class RecordFormatMigrationIT
{
    @Inject
    private TestDirectory testDirectory;
    private Path databaseDirectory;

    @BeforeEach
    void setUp()
    {
        databaseDirectory = testDirectory.homePath();
    }

    @Test
    void failToDowngradeFormatWhenUpgradeNotAllowed()
    {
        DatabaseManagementService managementService = startManagementService( StandardV5_0.NAME );
        GraphDatabaseAPI database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();
        managementService = startManagementService( StandardV4_3.NAME );
        database = getDefaultDatabase( managementService );
        try
        {
            Throwable throwable = assertDefaultDatabaseFailed( database );
            assertSame( UpgradeNotAllowedException.class, getRootCause( throwable ).getClass() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void failToDowngradeFormatWheUpgradeAllowed()
    {
        DatabaseManagementService managementService = startManagementService( StandardV5_0.NAME );
        GraphDatabaseAPI database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startDatabaseServiceWithUpgrade( databaseDirectory, StandardV4_3.NAME );
        database = getDefaultDatabase( managementService );
        try
        {
            Throwable throwable = assertDefaultDatabaseFailed( database );
            assertSame( StoreUpgrader.AttemptedDowngradeException.class, getRootCause( throwable ).getClass() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void skipMigrationIfFormatSpecifiedInConfig()
    {
        DatabaseManagementService managementService = startManagementService( StandardV4_3.NAME );
        GraphDatabaseAPI database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startManagementService( StandardV4_3.NAME );
        GraphDatabaseAPI nonUpgradedStore = getDefaultDatabase( managementService );
        RecordStorageEngine storageEngine = nonUpgradedStore.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        assertEquals( StandardV4_3.NAME, storageEngine.testAccessNeoStores().getRecordFormats().name() );
        managementService.shutdown();
    }

    @Test
    void requestMigrationIfStoreFormatNotSpecifiedButIsAvailableInRuntime()
    {
        DatabaseManagementService managementService = startManagementService( StandardV4_3.NAME );
        GraphDatabaseAPI database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = new TestDatabaseManagementServiceBuilder( databaseDirectory ).build();
        database = getDefaultDatabase( managementService );
        try
        {
            assertDefaultDatabaseFailed( database );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void latestRecordNotMigratedWhenFormatBumped()
    {
        DatabaseManagementService managementService = startManagementService( StandardV4_3.NAME );
        GraphDatabaseAPI database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startManagementService( Standard.LATEST_NAME );
        database = getDefaultDatabase( managementService );
        try
        {
            Throwable exception = assertDefaultDatabaseFailed( database );
            assertSame( UpgradeNotAllowedException.class, getRootCause( exception ).getClass() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static Throwable assertDefaultDatabaseFailed( GraphDatabaseAPI database )
    {
        assertThrows( Throwable.class, database::beginTx );
        DatabaseStateService dbStateService = getDatabaseStateService( database );
        var failure = dbStateService.causeOfFailure( database.databaseId() );
        return failure.orElseThrow( () -> new AssertionError( format( "No failure found for database %s", database.databaseId().name() ) ) );
    }

    private static DatabaseStateService getDatabaseStateService( GraphDatabaseAPI databaseService )
    {
        return databaseService.getDependencyResolver().resolveDependency( DatabaseStateService.class );
    }

    private static GraphDatabaseAPI getDefaultDatabase( DatabaseManagementService managementService )
    {
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private DatabaseManagementService startManagementService( String name )
    {
        return new TestDatabaseManagementServiceBuilder( databaseDirectory )
                .setConfig( record_format, name )
                .setConfig( storage_engine, RecordStorageEngineFactory.NAME )
                .build();
    }

    private static DatabaseManagementService startDatabaseServiceWithUpgrade( Path storeDir, String formatName )
    {
        return new TestDatabaseManagementServiceBuilder( storeDir )
                .setConfig( storage_engine, RecordStorageEngineFactory.NAME )
                .setConfig( record_format, formatName )
                .setConfig( allow_upgrade, true )
                .build();
    }
}
