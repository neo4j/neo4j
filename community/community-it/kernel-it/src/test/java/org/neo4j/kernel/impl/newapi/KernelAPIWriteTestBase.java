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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Answers;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.EntityCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.Value;

/**
 * KernelAPIWriteTestBase is the basis of write tests targeting the Kernel API.
 *
 * Just as with KernelAPIReadTestBase, write tests cannot provide all the functionality needed to construct the
 * test kernel, and also do not know how to assert the effects of the writes. These things are abstracted behind the
 * KernelAPIWriteTestSupport interface, which needs to be implemented to test Kernel write implementations.
 *
 * Since write tests modify the graph, the test graph is recreated on every test run.
 *
 * @param <WriteSupport> The test support for the current test.
 */
@SuppressWarnings("WeakerAccess")
@TestDirectoryExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class KernelAPIWriteTestBase<WriteSupport extends KernelAPIWriteTestSupport> {
    protected KernelAPIWriteTestSupport testSupport;
    protected GraphDatabaseService graphDb;

    // the following static fields are needed to create a fake internal transaction
    private static final TokenHolders tokenHolders = mock(TokenHolders.class);
    private static final QueryExecutionEngine engine = mock(QueryExecutionEngine.class);
    private static final TransactionalContextFactory contextFactory = mock(TransactionalContextFactory.class);
    private static final DatabaseAvailabilityGuard availabilityGuard = mock(DatabaseAvailabilityGuard.class);
    private static final ElementIdMapper elementIdMapper = mock(ElementIdMapper.class, Answers.RETURNS_MOCKS);

    @Inject
    private TestDirectory testDirectory;

    /**
     * Creates a new instance of WriteSupport, which will be used to execute the concrete test
     */
    public abstract WriteSupport newTestSupport();

    @BeforeAll
    public void setupGraph() {
        testSupport = newTestSupport();
        testSupport.setup(testDirectory.homePath(), this::createSystemGraph);
        graphDb = testSupport.graphBackdoor();
    }

    @BeforeEach
    public void clearGraph() {
        testSupport.clearGraph();
    }

    @AfterAll
    public void tearDown() {
        testSupport.tearDown();
    }

    /**
     * Setup privileges in the system graph which all test in the class will be using. The graph is only built once,
     * regardless of the number of tests.
     *
     * @param graphDb a graph API which should be used to build the system test graph
     */
    public void createSystemGraph(GraphDatabaseService graphDb) {}

    protected KernelTransaction beginTransaction() throws TransactionFailureException {
        return beginTransaction(LoginContext.AUTH_DISABLED);
    }

    protected KernelTransaction beginTransaction(LoginContext loginContext) throws TransactionFailureException {
        Kernel kernel = testSupport.kernelToTest();
        KernelTransaction kernelTransaction = kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, loginContext);
        new TransactionImpl(
                tokenHolders, contextFactory, availabilityGuard, engine, kernelTransaction, elementIdMapper);
        return kernelTransaction;
    }

    protected void transaction(ThrowingConsumer<KernelTransaction, Exception> action) throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            action.accept(tx);
            tx.commit();
        }
    }

    protected static CursorFactory cursorFactory(KernelTransaction ktx) {
        return ktx.cursors();
    }

    protected void assertProperties(
            EntityCursor entityCursor, PropertyCursor propertyCursor, IntObjectMap<Value> expectedProperties) {
        entityCursor.properties(propertyCursor, PropertySelection.ALL_PROPERTIES);
        MutableIntObjectMap<Value> readProperties = IntObjectMaps.mutable.empty();
        while (propertyCursor.next()) {
            readProperties.put(propertyCursor.propertyKey(), propertyCursor.propertyValue());
        }
        assertThat(readProperties).isEqualTo(expectedProperties);
    }

    protected boolean isNodeBased() {
        try (var tx = beginTransaction()) {
            return isNodeBased(tx);
        } catch (KernelException ex) {
            fail(
                    "Unable to determine whether the transaction would create a cursor over the relationship type index that is node-based");
            return false;
        }
    }

    protected boolean isNodeBased(KernelTransaction tx) {
        var cursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT);
        return cursor instanceof DefaultNodeBasedRelationshipTypeIndexCursor;
    }
}
