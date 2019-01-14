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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.time.LocalDate;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.DateValue;

import static migration.RecordFormatMigrationIT.startDatabaseWithFormat;
import static migration.RecordFormatMigrationIT.startNonUpgradableDatabaseWithFormat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

public class TemporalPropertiesRecordFormatIT
{
    // TODO tests for temporal arrays

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void failToCreateDateOnOldDatabase() throws Exception
    {
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService nonUpgradedStore = startNonUpgradableDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = nonUpgradedStore.beginTx() )
        {
            Node node = nonUpgradedStore.createNode();
            node.setProperty( "a", DateValue.date( 1991, 5, 3 ).asObjectCopy() );
            transaction.success();
        }
        catch ( TransactionFailureException e )
        {
            assertEquals( "Current record format does not support TEMPORAL_PROPERTIES. Please upgrade your store " +
                    "to the format that support requested capability.", Exceptions.rootCause( e ).getMessage() );
        }
        nonUpgradedStore.shutdown();

        GraphDatabaseService restartedOldFormatDatabase = startNonUpgradableDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = restartedOldFormatDatabase.beginTx() )
        {
            Node node = restartedOldFormatDatabase.createNode();
            node.setProperty( "c", "d" );
            transaction.success();
        }
        restartedOldFormatDatabase.shutdown();
    }

    @Test
    public void failToCreateDateArrayOnOldDatabase() throws Exception
    {
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService nonUpgradedStore = startNonUpgradableDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        LocalDate date = DateValue.date( 1991, 5, 3 ).asObjectCopy();
        try ( Transaction transaction = nonUpgradedStore.beginTx() )
        {
            Node node = nonUpgradedStore.createNode();
            node.setProperty( "a", new LocalDate[]{date, date} );
            transaction.success();
        }
        catch ( TransactionFailureException e )
        {
            assertEquals( "Current record format does not support TEMPORAL_PROPERTIES. Please upgrade your store " +
                    "to the format that support requested capability.", Exceptions.rootCause( e ).getMessage() );
        }
        nonUpgradedStore.shutdown();

        GraphDatabaseService restartedOldFormatDatabase = startNonUpgradableDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        try ( Transaction transaction = restartedOldFormatDatabase.beginTx() )
        {
            Node node = restartedOldFormatDatabase.createNode();
            node.setProperty( "c", "d" );
            transaction.success();
        }
        restartedOldFormatDatabase.shutdown();
    }

    @Test
    public void createDatePropertyOnLatestDatabase()
    {
        File storeDir = testDirectory.graphDbDir();
        Label label = Label.label( "DateNode" );
        String propertyKey = "a";
        LocalDate date = DateValue.date( 1991, 5, 3 ).asObjectCopy();

        GraphDatabaseService database = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( propertyKey, date );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseService restartedDatabase = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction ignored = restartedDatabase.beginTx() )
        {
            assertNotNull( restartedDatabase.findNode( label, propertyKey, date ) );
        }
        restartedDatabase.shutdown();
    }

    @Test
    public void createDateArrayOnLatestDatabase() throws Exception
    {
        File storeDir = testDirectory.graphDbDir();
        Label label = Label.label( "DateNode" );
        String propertyKey = "a";
        LocalDate date = DateValue.date( 1991, 5, 3 ).asObjectCopy();

        GraphDatabaseService database = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( propertyKey, new LocalDate[]{date, date} );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseService restartedDatabase = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction ignored = restartedDatabase.beginTx() )
        {
            try ( ResourceIterator<Node> nodes = restartedDatabase.findNodes( label ) )
            {
                Node node = nodes.next();
                LocalDate[] points = (LocalDate[]) node.getProperty( propertyKey );
                assertThat( points, arrayWithSize( 2 ) );
            }
        }
        restartedDatabase.shutdown();
    }

    @Test
    public void failToOpenStoreWithDatePropertyUsingOldFormat()
    {
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService database = startDatabaseWithFormat( storeDir, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", DateValue.date( 1991, 5, 3 ) );
            transaction.success();
        }
        database.shutdown();

        try
        {
            startDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        }
        catch ( Throwable t )
        {
            assertSame( StoreUpgrader.AttemptedDowngradeException.class, Exceptions.rootCause( t ).getClass() );
        }
    }
}
