/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

public class RecordFormatMigrationIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void skipMigrationIfFormatSpecifiedInConfig() throws Exception
    {
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService database = startDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseAPI nonUpgradedStore = (GraphDatabaseAPI) startDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        RecordStorageEngine storageEngine = nonUpgradedStore.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        assertEquals( StandardV3_2.NAME, storageEngine.testAccessNeoStores().getRecordFormats().name() );
        nonUpgradedStore.shutdown();
    }

    @Test
    public void skipMigrationIfStoreFormatNotSpecifiedButIsAvailableInRuntime() throws Exception
    {
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService database = startDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", "b" );
            transaction.success();
        }
        database.shutdown();

        try
        {
            GraphDatabaseAPI nonUpgradedStore = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
            RecordStorageEngine storageEngine = nonUpgradedStore.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
            assertEquals( StandardV3_2.NAME, storageEngine.testAccessNeoStores().getRecordFormats().name() );
            nonUpgradedStore.shutdown();
        }
        catch ( Throwable e )
        {
            assertThat( Exceptions.rootCause( e ).getMessage(), startsWith( "Neo4j cannot be started because the " +
                    "database files require upgrading and upgrades are disabled in the configuration." ) );
        }
    }

    private GraphDatabaseService startDatabaseWithFormat( File storeDir, String format )
    {
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( record_format, format ).newGraphDatabase();
    }
}
