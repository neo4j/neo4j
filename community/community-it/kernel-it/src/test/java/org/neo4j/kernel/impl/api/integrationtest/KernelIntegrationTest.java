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
package org.neo4j.kernel.impl.api.integrationtest;

import static java.util.Collections.emptyIterator;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.nio.file.Path;
import java.util.Iterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;

@TestDirectoryExtension
public abstract class KernelIntegrationTest {
    @Inject
    protected TestDirectory testDir;

    protected GraphDatabaseAPI db;
    protected Kernel kernel;
    protected IndexingService indexingService;

    protected InternalTransaction transaction;
    protected KernelTransaction kernelTransaction;
    protected DependencyResolver dependencyResolver;
    private DatabaseManagementService managementService;

    public TokenWrite tokenWriteInNewTransaction() {
        beginTransaction(AnonymousContext.writeToken());
        return kernelTransaction.tokenWrite();
    }

    protected Write dataWriteInNewTransaction() throws KernelException {
        beginTransaction(AnonymousContext.write());
        return kernelTransaction.dataWrite();
    }

    public SchemaWrite schemaWriteInNewTransaction() throws KernelException {
        beginTransaction(AUTH_DISABLED);
        return kernelTransaction.schemaWrite();
    }

    protected Procedures procs() {
        if (kernelTransaction == null) {
            beginTransaction(AnonymousContext.read());
        }
        return kernelTransaction.procedures();
    }

    protected KernelTransaction newTransaction() {
        beginTransaction(AnonymousContext.read());
        return kernelTransaction;
    }

    protected KernelTransaction newTransaction(LoginContext loginContext) {
        beginTransaction(loginContext);
        return kernelTransaction;
    }

    /**
     * Create a temporary section wherein other transactions can be started an committed, and after which the <em>current</em> transaction will be restored as
     * current.
     */
    protected Resource captureTransaction() {
        InternalTransaction tx = transaction;
        KernelTransaction ktx = kernelTransaction;
        return () -> {
            transaction = tx;
            kernelTransaction = ktx;
        };
    }

    public long commit() {
        transaction.commit();
        long txId = kernelTransaction.getTransactionId();
        transaction = null;
        kernelTransaction = null;
        return txId;
    }

    protected void rollback() {
        transaction.rollback();
        transaction = null;
        kernelTransaction = null;
    }

    private void beginTransaction(LoginContext context) {
        transaction = db.beginTransaction(IMPLICIT, context);
        kernelTransaction = transaction.kernelTransaction();
    }

    @BeforeEach
    public void setup() {
        startDb();
    }

    @AfterEach
    public void cleanup() throws Exception {
        stopDb();
    }

    public String getDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    private void startDb() {
        managementService = createDatabaseService();
        db = (GraphDatabaseAPI) managementService.database(getDatabaseName());
        dependencyResolver = db.getDependencyResolver();
        kernel = dependencyResolver.resolveDependency(Kernel.class);
        indexingService = dependencyResolver.resolveDependency(IndexingService.class);
    }

    protected GraphDatabaseAPI openDatabase(String databaseName) {
        return (GraphDatabaseAPI) managementService.database(databaseName);
    }

    protected DatabaseManagementService createDatabaseService() {
        TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder =
                configure(createGraphDatabaseFactory(testDir.homePath())).setFileSystem(testDir.getFileSystem());
        return configure(databaseManagementServiceBuilder).build();
    }

    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory(Path databaseRootDir) {
        return new TestDatabaseManagementServiceBuilder(databaseRootDir);
    }

    protected TestDatabaseManagementServiceBuilder configure(
            TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder) {
        return databaseManagementServiceBuilder;
    }

    void dbWithNoCache() throws TransactionFailureException {
        stopDb();
        startDb();
    }

    private void stopDb() throws TransactionFailureException {
        if (kernelTransaction != null && kernelTransaction.isOpen()) {
            kernelTransaction.close();
        }
        managementService.shutdown();
    }

    protected void restartDb() throws TransactionFailureException {
        stopDb();
        startDb();
    }

    static Value relationshipGetProperty(KernelTransaction transaction, long relationship, int property) {
        try (RelationshipScanCursor cursor =
                        transaction.cursors().allocateRelationshipScanCursor(transaction.cursorContext());
                PropertyCursor properties = transaction
                        .cursors()
                        .allocatePropertyCursor(transaction.cursorContext(), transaction.memoryTracker())) {
            transaction.dataRead().singleRelationship(relationship, cursor);
            if (!cursor.next()) {
                return NO_VALUE;
            } else {
                cursor.properties(properties, PropertySelection.selection(property));
                return properties.next() ? properties.propertyValue() : NO_VALUE;
            }
        }
    }

    static Iterator<Long> nodeGetRelationships(KernelTransaction transaction, long node, Direction direction) {
        return nodeGetRelationships(transaction, node, direction, null);
    }

    static Iterator<Long> nodeGetRelationships(
            KernelTransaction transaction, long node, Direction direction, int[] types) {
        try (NodeCursor cursor = transaction.cursors().allocateNodeCursor(transaction.cursorContext())) {
            transaction.dataRead().singleNode(node, cursor);
            if (!cursor.next()) {
                return emptyIterator();
            }

            return switch (direction) {
                case OUTGOING -> outgoingIterator(
                        transaction.cursors(),
                        cursor,
                        types,
                        RelationshipDataAccessor::relationshipReference,
                        transaction.cursorContext());
                case INCOMING -> incomingIterator(
                        transaction.cursors(),
                        cursor,
                        types,
                        RelationshipDataAccessor::relationshipReference,
                        transaction.cursorContext());
                case BOTH -> allIterator(
                        transaction.cursors(),
                        cursor,
                        types,
                        RelationshipDataAccessor::relationshipReference,
                        transaction.cursorContext());
            };
        }
    }

    protected static int countNodes(KernelTransaction transaction) {
        int result = 0;
        try (NodeCursor cursor = transaction.cursors().allocateNodeCursor(transaction.cursorContext())) {
            transaction.dataRead().allNodesScan(cursor);
            while (cursor.next()) {
                result++;
            }
        }
        return result;
    }

    public static int countRelationships(KernelTransaction transaction) {
        int result = 0;
        try (RelationshipScanCursor cursor =
                transaction.cursors().allocateRelationshipScanCursor(transaction.cursorContext())) {
            transaction.dataRead().allRelationshipsScan(cursor);
            while (cursor.next()) {
                result++;
            }
        }
        return result;
    }

    KernelImpl internalKernel() {
        return (KernelImpl) kernel;
    }
}
