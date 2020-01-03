/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

@ExtendWith( TestDirectoryExtension.class )
class RecordFormatMigrationIT
{
    @Inject
    private TestDirectory testDirectory;
    private File storeDir;

    @BeforeEach
    void setUp()
    {
        storeDir = testDirectory.storeDir();
    }

    @Test
    void failToDowngradeFormatWhenUpgradeNotAllowed()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( storeDir, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();
        Throwable throwable = assertThrows( Throwable.class, () -> startDatabaseWithFormatUnspecifiedUpgrade( storeDir, StandardV3_2.NAME ) );
        assertSame( UpgradeNotAllowedByConfigurationException.class, Exceptions.rootCause( throwable ).getClass() );
    }

    @Test
    void failToDowngradeFormatWheUpgradeAllowed()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( storeDir, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();
        Throwable throwable = assertThrows( Throwable.class,
                () -> new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                        .setConfig( record_format, StandardV3_2.NAME )
                        .setConfig( allow_upgrade, Settings.TRUE ).newGraphDatabase() );
        assertSame( StoreUpgrader.AttemptedDowngradeException.class, Exceptions.rootCause( throwable ).getClass() );
    }

    @Test
    void skipMigrationIfFormatSpecifiedInConfig()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseAPI nonUpgradedStore = (GraphDatabaseAPI) startDatabaseWithFormatUnspecifiedUpgrade( storeDir, StandardV3_2.NAME );
        RecordStorageEngine storageEngine = nonUpgradedStore.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        assertEquals( StandardV3_2.NAME, storageEngine.testAccessNeoStores().getRecordFormats().name() );
        nonUpgradedStore.shutdown();
    }

    @Test
    void skipMigrationIfStoreFormatNotSpecifiedButIsAvailableInRuntime()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseAPI nonUpgradedStore = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabase( storeDir );
        RecordStorageEngine storageEngine = nonUpgradedStore.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        assertEquals( StandardV3_2.NAME, storageEngine.testAccessNeoStores().getRecordFormats().name() );
        nonUpgradedStore.shutdown();
    }

    @Test
    void latestRecordNotMigratedWhenFormatBumped()
    {
        GraphDatabaseService database = startDatabaseWithFormatUnspecifiedUpgrade( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        Throwable exception = assertThrows( Throwable.class, () -> startDatabaseWithFormatUnspecifiedUpgrade( storeDir, Standard.LATEST_NAME ) );
        assertSame( UpgradeNotAllowedByConfigurationException.class, Exceptions.rootCause( exception ).getClass() );
    }

    private static GraphDatabaseService startDatabaseWithFormatUnspecifiedUpgrade( File storeDir, String formatName )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, formatName ).newGraphDatabase();
    }

    static GraphDatabaseService startNonUpgradableDatabaseWithFormat( File storeDir, String formatName )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, formatName )
                .setConfig( allow_upgrade, FALSE ).newGraphDatabase();
    }

    static GraphDatabaseService startDatabaseWithFormat( File storeDir, String formatName )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, formatName )
                .setConfig( allow_upgrade, TRUE ).newGraphDatabase();
    }
}
