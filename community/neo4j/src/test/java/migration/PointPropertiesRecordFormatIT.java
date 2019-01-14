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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

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
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.PointValue;

import static migration.RecordFormatMigrationIT.startDatabaseWithFormat;
import static migration.RecordFormatMigrationIT.startNonUpgradableDatabaseWithFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.Values.pointValue;

@ExtendWith( TestDirectoryExtension.class )
public class PointPropertiesRecordFormatIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void failToCreatePointOnOldDatabase()
    {
        File storeDir = testDirectory.storeDir();
        GraphDatabaseService nonUpgradedStore = startNonUpgradableDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        TransactionFailureException exception = assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction transaction = nonUpgradedStore.beginTx() )
            {
                Node node = nonUpgradedStore.createNode();
                node.setProperty( "a", pointValue( Cartesian, 1.0, 2.0 ) );
                transaction.success();
            }
        } );
        assertEquals( "Current record format does not support POINT_PROPERTIES. Please upgrade your store to the format that support requested capability.",
                Exceptions.rootCause( exception ).getMessage() );
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
    void failToCreatePointArrayOnOldDatabase()
    {
        File storeDir = testDirectory.storeDir();
        GraphDatabaseService nonUpgradedStore = startNonUpgradableDatabaseWithFormat( storeDir, StandardV3_2.NAME );
        PointValue point = pointValue( Cartesian, 1.0, 2.0 );
        TransactionFailureException exception = assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction transaction = nonUpgradedStore.beginTx() )
            {
                Node node = nonUpgradedStore.createNode();
                node.setProperty( "a", new PointValue[]{point, point} );
                transaction.success();
            }
        } );
        assertEquals( "Current record format does not support POINT_PROPERTIES. Please upgrade your store to the format that support requested capability.",
                Exceptions.rootCause( exception ).getMessage() );
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
    void createPointPropertyOnLatestDatabase()
    {
        File storeDir = testDirectory.storeDir();
        Label pointNode = Label.label( "PointNode" );
        String propertyKey = "a";
        PointValue pointValue = pointValue( Cartesian, 1.0, 2.0 );

        GraphDatabaseService database = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( pointNode );
            node.setProperty( propertyKey, pointValue );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseService restartedDatabase = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction ignored = restartedDatabase.beginTx() )
        {
            assertNotNull( restartedDatabase.findNode( pointNode, propertyKey, pointValue ) );
        }
        restartedDatabase.shutdown();
    }

    @Test
    void createPointArrayPropertyOnLatestDatabase()
    {
        File storeDir = testDirectory.storeDir();
        Label pointNode = Label.label( "PointNode" );
        String propertyKey = "a";
        PointValue pointValue = pointValue( Cartesian, 1.0, 2.0 );

        GraphDatabaseService database = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( pointNode );
            node.setProperty( propertyKey, new PointValue[]{pointValue, pointValue} );
            transaction.success();
        }
        database.shutdown();

        GraphDatabaseService restartedDatabase = startDatabaseWithFormat( storeDir, Standard.LATEST_NAME );
        try ( Transaction ignored = restartedDatabase.beginTx() )
        {
            try ( ResourceIterator<Node> nodes = restartedDatabase.findNodes( pointNode ) )
            {
                Node node = nodes.next();
                PointValue[] points = (PointValue[]) node.getProperty( propertyKey );
                assertThat( points, arrayWithSize( 2 ) );
            }
        }
        restartedDatabase.shutdown();
    }

    @Test
    void failToOpenStoreWithPointPropertyUsingOldFormat()
    {
        File storeDir = testDirectory.storeDir();
        GraphDatabaseService database = startDatabaseWithFormat( storeDir, StandardV3_4.NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "a", pointValue( Cartesian, 1.0, 2.0 ) );
            transaction.success();
        }
        database.shutdown();

        Throwable throwable = assertThrows( Throwable.class, () -> startDatabaseWithFormat( storeDir, StandardV3_2.NAME ) );
        assertSame( StoreUpgrader.AttemptedDowngradeException.class, Exceptions.rootCause( throwable ).getClass() );
    }
}
