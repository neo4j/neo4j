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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * KernelAPIReadTestBase is the basis of read tests targeting the Kernel API.
 *
 * As tests are packaged together with the API, they cannot provide all the functionality needed to construct the
 * test graph, or provide the concrete Kernel to test. These things are abstracted behind the
 * KernelAPIReadTestSupport interface, which needs to be implemented to test reading Kernel implementations.
 *
 * As read tests do not modify the graph, the test graph is created lazily on the first test run.
 *
 * @param <ReadSupport> The test support for the current test.
 */
@SuppressWarnings("WeakerAccess")
@TestDirectoryExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class KernelAPIReadTestBase<ReadSupport extends KernelAPIReadTestSupport> {
    protected KernelAPIReadTestSupport testSupport;
    protected KernelTransaction tx;
    protected Read read;
    protected SchemaRead schemaRead;
    protected Token token;
    protected ManagedTestCursors cursors;

    @Inject
    private TestDirectory testDirectory;

    /**
     * Creates a new instance of KernelAPIReadTestSupport, which will be used to execute the concrete test
     */
    public abstract ReadSupport newTestSupport();

    /**
     * Create the graph which all test in the class will be executed against. The graph is only built once,
     * regardless of the number of tests.
     *
     * @param graphDb a graph API which should be used to build the test graph
     */
    public abstract void createTestGraph(GraphDatabaseService graphDb);

    /**
     * Setup privileges in the system graph which all test in the class will be using. The graph is only built once,
     * regardless of the number of tests.
     *
     * @param graphDb a graph API which should be used to build the system test graph
     */
    public void createSystemGraph(GraphDatabaseService graphDb) {}

    @BeforeAll
    public void setupGraph() {
        testSupport = newTestSupport();
        testSupport.setup(testDirectory.homePath(), this::createTestGraph, this::createSystemGraph);
    }

    @BeforeEach
    public void disableAuth() throws KernelException {
        changeUser(LoginContext.AUTH_DISABLED);
    }

    @AfterEach
    public void closeTransaction() throws Exception {
        tx.commit();
        cursors.assertAllClosedAndReset();
    }

    @AfterAll
    public void tearDown() {
        testSupport.tearDown();
    }

    protected void changeUser(LoginContext loginContext) throws KernelException {
        Kernel kernel = testSupport.kernelToTest();
        tx = beginTransaction(kernel, loginContext);
        token = tx.token();
        read = tx.dataRead();
        schemaRead = tx.schemaRead();
        cursors = new ManagedTestCursors(tx.cursors());
    }

    protected KernelTransaction beginTransaction() throws TransactionFailureException {
        Kernel kernel = testSupport.kernelToTest();
        return beginTransaction(kernel, LoginContext.AUTH_DISABLED);
    }

    protected KernelTransaction beginTransaction(LoginContext loginContext) throws TransactionFailureException {
        Kernel kernel = testSupport.kernelToTest();
        return beginTransaction(kernel, loginContext);
    }

    private static KernelTransaction beginTransaction(Kernel kernel, LoginContext loginContext)
            throws TransactionFailureException {
        return kernel.beginTransaction(KernelTransaction.Type.IMPLICIT, loginContext);
    }
}
