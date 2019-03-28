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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.helpers.Exceptions.rootCause;

@ExtendWith( TestDirectoryExtension.class )
class RecordFormatMigrationIT
{
    @Inject
    private TestDirectory testDirectory;
    private File databaseDirectory;

    @BeforeEach
    void setUp()
    {
        databaseDirectory = testDirectory.databaseDir();
    }

    @Test
    void failToDowngradeFormatWhenUpgradeNotAllowed()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();
        GraphDatabaseService databaseService = startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, StandardV4_0.NAME );
        try
        {
            DatabaseContext databaseContext = assertDefaultDatabaseFailed( databaseService );
            Throwable throwable = databaseContext.failureCause();
            assertSame( UpgradeNotAllowedException.class, rootCause( throwable ).getClass() );
        }
        finally
        {
            databaseService.shutdown();
        }
    }

    @Test
    void failToDowngradeFormatWheUpgradeAllowed()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, StandardV4_0.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( databaseDirectory )
                .setConfig( record_format, StandardV3_4.NAME )
                .setConfig( allow_upgrade, TRUE )
                .newGraphDatabase();
        try
        {
            DatabaseContext databaseContext = assertDefaultDatabaseFailed( databaseService );
            Throwable throwable = databaseContext.failureCause();
            assertSame( StoreUpgrader.AttemptedDowngradeException.class, rootCause( throwable ).getClass() );
        }
        finally
        {
            databaseService.shutdown();
        }
    }

    @Test
    void skipMigrationIfFormatSpecifiedInConfig()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseAPI nonUpgradedStore = (GraphDatabaseAPI) startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, StandardV3_4.NAME );
        RecordStorageEngine storageEngine = nonUpgradedStore.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        assertEquals( StandardV3_4.NAME, storageEngine.testAccessNeoStores().getRecordFormats().name() );
        nonUpgradedStore.shutdown();
    }

    @Test
    void requestMigrationIfStoreFormatNotSpecifiedButIsAvailableInRuntime()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase( databaseDirectory );
        try
        {
            assertDefaultDatabaseFailed( databaseService );
        }
        finally
        {
            databaseService.shutdown();
        }
    }

    @Test
    void latestRecordNotMigratedWhenFormatBumped()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseService databaseService = startDatabaseWithFormatUnspecifiedUpgrade( databaseDirectory, Standard.LATEST_NAME );
        try
        {
            DatabaseContext databaseContext = assertDefaultDatabaseFailed( databaseService );
            Throwable exception = databaseContext.failureCause();
            assertSame( UpgradeNotAllowedException.class, rootCause( exception ).getClass() );
        }
        finally
        {
            databaseService.shutdown();
        }
    }

    private static GraphDatabaseService startDatabaseWithFormatUnspecifiedUpgrade( File storeDir, String formatName )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, formatName ).newGraphDatabase();
    }

    private DatabaseContext assertDefaultDatabaseFailed( GraphDatabaseService databaseService )
    {
        assertThrows( Throwable.class, databaseService::beginTx );
        DatabaseManager<?> databaseManager = getDatabaseManager( (GraphDatabaseAPI) databaseService );
        DatabaseContext databaseContext = databaseManager.getDatabaseContext( new DatabaseId( testDirectory.databaseLayout().getDatabaseName() ) ).get();
        assertTrue( databaseContext.isFailed() );
        return databaseContext;
    }

    private DatabaseManager<?> getDatabaseManager( GraphDatabaseAPI databaseService )
    {
        return databaseService.getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    static GraphDatabaseService startDatabaseWithFormat( File storeDir, String formatName )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, formatName )
                .setConfig( allow_upgrade, TRUE ).newGraphDatabase();
    }
}
