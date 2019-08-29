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

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.PointValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.Values.pointValue;

@TestDirectoryExtension
class PointPropertiesRecordFormatIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void createPointPropertyOnLatestDatabase()
    {
        File storeDir = testDirectory.storeDir();
        Label pointNode = Label.label( "PointNode" );
        String propertyKey = "a";
        PointValue pointValue = pointValue( Cartesian, 1.0, 2.0 );

        DatabaseManagementService managementService = startDatabaseServiceWithUpgrade( storeDir, Standard.LATEST_NAME );
        GraphDatabaseService database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( pointNode );
            node.setProperty( propertyKey, pointValue );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startDatabaseServiceWithUpgrade( storeDir, Standard.LATEST_NAME );
        GraphDatabaseService restartedDatabase = getDefaultDatabase( managementService );
        try ( Transaction ignored = restartedDatabase.beginTx() )
        {
            assertNotNull( restartedDatabase.findNode( pointNode, propertyKey, pointValue ) );
        }
        managementService.shutdown();
    }

    @Test
    void createPointArrayPropertyOnLatestDatabase()
    {
        File storeDir = testDirectory.storeDir();
        Label pointNode = Label.label( "PointNode" );
        String propertyKey = "a";
        PointValue pointValue = pointValue( Cartesian, 1.0, 2.0 );

        DatabaseManagementService managementService = startDatabaseServiceWithUpgrade( storeDir, Standard.LATEST_NAME );
        GraphDatabaseService database = getDefaultDatabase( managementService );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( pointNode );
            node.setProperty( propertyKey, new PointValue[]{pointValue, pointValue} );
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startDatabaseServiceWithUpgrade( storeDir, Standard.LATEST_NAME );
        GraphDatabaseService restartedDatabase = getDefaultDatabase( managementService );
        try ( Transaction ignored = restartedDatabase.beginTx() )
        {
            try ( ResourceIterator<Node> nodes = restartedDatabase.findNodes( pointNode ) )
            {
                Node node = nodes.next();
                PointValue[] points = (PointValue[]) node.getProperty( propertyKey );
                assertThat( points, arrayWithSize( 2 ) );
            }
        }
        managementService.shutdown();
    }

    private static DatabaseManagementService startDatabaseServiceWithUpgrade( File storeDir, String formatName )
    {
        return new DatabaseManagementServiceBuilder( storeDir ).setConfig( record_format, formatName )
                .setConfig( allow_upgrade, true ).build();
    }

    private static GraphDatabaseService getDefaultDatabase( DatabaseManagementService managementService )
    {
        return managementService.database( DEFAULT_DATABASE_NAME );
    }
}
