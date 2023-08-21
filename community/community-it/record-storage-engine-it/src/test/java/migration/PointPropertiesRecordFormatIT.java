/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.Values.pointValue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.PointValue;

@TestDirectoryExtension
class PointPropertiesRecordFormatIT {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void createPointPropertyOnLatestDatabase() {
        Path storeDir = testDirectory.homePath();
        Label pointNode = Label.label("PointNode");
        String propertyKey = "a";
        PointValue pointValue = pointValue(CARTESIAN, 1.0, 2.0);

        DatabaseManagementService managementService = startDatabaseService(storeDir);
        GraphDatabaseService database = getDefaultDatabase(managementService);
        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode(pointNode);
            node.setProperty(propertyKey, pointValue);
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startDatabaseService(storeDir);
        GraphDatabaseService restartedDatabase = getDefaultDatabase(managementService);
        try (Transaction transaction = restartedDatabase.beginTx()) {
            assertNotNull(transaction.findNode(pointNode, propertyKey, pointValue));
        }
        managementService.shutdown();
    }

    @Test
    void createPointArrayPropertyOnLatestDatabase() {
        Path storeDir = testDirectory.homePath();
        Label pointNode = Label.label("PointNode");
        String propertyKey = "a";
        PointValue pointValue = pointValue(CARTESIAN, 1.0, 2.0);

        DatabaseManagementService managementService = startDatabaseService(storeDir);
        GraphDatabaseService database = getDefaultDatabase(managementService);
        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode(pointNode);
            node.setProperty(propertyKey, new PointValue[] {pointValue, pointValue});
            transaction.commit();
        }
        managementService.shutdown();

        managementService = startDatabaseService(storeDir);
        GraphDatabaseService restartedDatabase = getDefaultDatabase(managementService);
        try (Transaction tx = restartedDatabase.beginTx()) {
            try (ResourceIterator<Node> nodes = tx.findNodes(pointNode)) {
                Node node = nodes.next();
                PointValue[] points = (PointValue[]) node.getProperty(propertyKey);
                assertThat(points).hasSize(2);
            }
        }
        managementService.shutdown();
    }

    private static DatabaseManagementService startDatabaseService(Path storeDir) {
        return new TestDatabaseManagementServiceBuilder(storeDir)
                .setConfig(db_format, FormatFamily.ALIGNED.name())
                .build();
    }

    private static GraphDatabaseService getDefaultDatabase(DatabaseManagementService managementService) {
        return managementService.database(DEFAULT_DATABASE_NAME);
    }
}
