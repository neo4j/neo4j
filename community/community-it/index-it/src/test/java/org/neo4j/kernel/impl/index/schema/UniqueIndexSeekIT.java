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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.impl.index.schema.tracking.TrackingReadersIndexAccessor.numberOfClosedReaders;
import static org.neo4j.kernel.impl.index.schema.tracking.TrackingReadersIndexAccessor.numberOfOpenReaders;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.index.schema.tracking.TrackingIndexExtensionFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class UniqueIndexSeekIT {
    private static final String NODE_CONSTRAINT_NAME = "uniqueNodeConstraint";
    private static final String RELATIONSHIP_CONSTRAINT_NAME = "uniqueRelationshipConstraint";

    @Inject
    private TestDirectory directory;

    private DatabaseManagementService managementService;

    @Test
    void nodeUniqueIndexSeekDoNotLeakIndexReaders() throws KernelException {
        TrackingIndexExtensionFactory indexExtensionFactory =
                new TrackingIndexExtensionFactory(new RangeIndexProviderFactory());
        GraphDatabaseAPI database = createDatabase(indexExtensionFactory);
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        Config config = dependencyResolver.resolveDependency(Config.class);
        try {
            Label label = label("spaceship");
            String nameProperty = "name";
            createNodeUniqueConstraint(database, label, nameProperty);

            generateRandomData(database, label, nameProperty);

            assertNotNull(indexExtensionFactory.getIndexProvider(config.get(initial_default_database)));
            assertThat(numberOfClosedReaders()).isGreaterThan(0L);
            assertThat(numberOfOpenReaders()).isGreaterThan(0L);
            assertThat(numberOfClosedReaders()).isCloseTo(numberOfOpenReaders(), within(1L));

            lockNodeUsingUniqueIndexSeek(database, nameProperty);

            assertThat(numberOfClosedReaders()).isCloseTo(numberOfOpenReaders(), within(1L));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void relationshipUniqueIndexSeekDoNotLeakIndexReaders() throws KernelException {
        TrackingIndexExtensionFactory indexExtensionFactory =
                new TrackingIndexExtensionFactory(new RangeIndexProviderFactory());
        GraphDatabaseAPI database = createDatabase(indexExtensionFactory);
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        Config config = dependencyResolver.resolveDependency(Config.class);
        try {
            RelationshipType type = RelationshipType.withName("spaceship");
            String nameProperty = "name";
            createRelationshipUniqueConstraint(database, type, nameProperty);

            generateRandomData(database, type, nameProperty);

            assertNotNull(indexExtensionFactory.getIndexProvider(config.get(initial_default_database)));
            assertThat(numberOfClosedReaders()).isGreaterThan(0L);
            assertThat(numberOfOpenReaders()).isGreaterThan(0L);
            assertThat(numberOfClosedReaders()).isCloseTo(numberOfOpenReaders(), within(1L));

            lockRelationshipUsingUniqueIndexSeek(database, nameProperty);

            assertThat(numberOfClosedReaders()).isCloseTo(numberOfOpenReaders(), within(1L));
        } finally {
            managementService.shutdown();
        }
    }

    private GraphDatabaseAPI createDatabase(TrackingIndexExtensionFactory indexExtensionFactory) {
        managementService = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .addExtension(indexExtensionFactory)
                .build();
        return (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private static void lockNodeUsingUniqueIndexSeek(GraphDatabaseAPI database, String nameProperty)
            throws KernelException {
        try (Transaction transaction = database.beginTx()) {
            KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            TokenRead tokenRead = kernelTransaction.tokenRead();
            Read dataRead = kernelTransaction.dataRead();

            int propertyId = tokenRead.propertyKey(nameProperty);
            IndexDescriptor indexReference = kernelTransaction.schemaRead().indexGetForName(NODE_CONSTRAINT_NAME);
            try (NodeValueIndexCursor cursor = kernelTransaction
                    .cursors()
                    .allocateNodeValueIndexCursor(
                            kernelTransaction.cursorContext(), kernelTransaction.memoryTracker())) {
                dataRead.lockingNodeUniqueIndexSeek(
                        indexReference, cursor, PropertyIndexQuery.ExactPredicate.exact(propertyId, "value"));
            }
            transaction.commit();
        }
    }

    private static void lockRelationshipUsingUniqueIndexSeek(GraphDatabaseAPI database, String nameProperty)
            throws KernelException {
        try (Transaction transaction = database.beginTx()) {
            KernelTransaction kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            TokenRead tokenRead = kernelTransaction.tokenRead();
            Read dataRead = kernelTransaction.dataRead();

            int propertyId = tokenRead.propertyKey(nameProperty);
            IndexDescriptor indexReference =
                    kernelTransaction.schemaRead().indexGetForName(RELATIONSHIP_CONSTRAINT_NAME);
            try (RelationshipValueIndexCursor cursor = kernelTransaction
                    .cursors()
                    .allocateRelationshipValueIndexCursor(
                            kernelTransaction.cursorContext(), kernelTransaction.memoryTracker())) {
                dataRead.lockingRelationshipUniqueIndexSeek(
                        indexReference, cursor, PropertyIndexQuery.ExactPredicate.exact(propertyId, "value"));
            }
            transaction.commit();
        }
    }

    private static void generateRandomData(GraphDatabaseAPI database, Label label, String nameProperty) {
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < 1000; i++) {
                Node node = transaction.createNode(label);
                node.setProperty(nameProperty, "PlanetExpress" + i);
            }
            transaction.commit();
        }
    }

    private static void generateRandomData(GraphDatabaseAPI database, RelationshipType type, String nameProperty) {
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < 1000; i++) {
                Relationship r = transaction.createNode().createRelationshipTo(transaction.createNode(), type);
                r.setProperty(nameProperty, "PlanetExpress" + i);
            }
            transaction.commit();
        }
    }

    private static void createNodeUniqueConstraint(GraphDatabaseAPI database, Label label, String nameProperty)
            throws KernelException {
        try (TransactionImpl transaction = (TransactionImpl) database.beginTx()) {
            IndexingTestUtil.createNodePropUniqueConstraintWithSpecifiedProvider(
                    transaction, TrackingIndexExtensionFactory.DESCRIPTOR, label, nameProperty, NODE_CONSTRAINT_NAME);
            transaction.commit();
        }
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            transaction.commit();
        }
    }

    private static void createRelationshipUniqueConstraint(
            GraphDatabaseAPI database, RelationshipType type, String nameProperty) throws KernelException {
        try (TransactionImpl transaction = (TransactionImpl) database.beginTx()) {
            IndexingTestUtil.createRelPropUniqueConstraintWithSpecifiedProvider(
                    transaction,
                    TrackingIndexExtensionFactory.DESCRIPTOR,
                    type,
                    nameProperty,
                    RELATIONSHIP_CONSTRAINT_NAME);
            transaction.commit();
        }
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            transaction.commit();
        }
    }
}
