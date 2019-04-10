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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

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
@SuppressWarnings( "WeakerAccess" )
@ExtendWith( TestDirectoryExtension.class )
public abstract class KernelAPIReadTestBase<ReadSupport extends KernelAPIReadTestSupport>
{
    protected static File folder;
    protected static KernelAPIReadTestSupport testSupport;
    protected Transaction tx;
    protected Read read;
    protected SchemaRead schemaRead;
    protected Token token;
    protected ManagedTestCursors cursors;

    @Inject
    private TestDirectory testDirectory;

    @AfterEach
    public void closeTransaction() throws Exception
    {
        tx.success();
        tx.close();
        cursors.assertAllClosedAndReset();
    }

    @AfterAll
    public static void tearDown()
    {
        if ( testSupport != null )
        {
            testSupport.tearDown();
            testSupport = null;
        }
    }

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
    public abstract void createTestGraph( GraphDatabaseService graphDb );

    @BeforeEach
    public void setupGraph() throws IOException, KernelException
    {
        if ( testSupport == null )
        {
            testSupport = newTestSupport();
            testSupport.setup( testDirectory.storeDir(), this::createTestGraph );
        }
        Kernel kernel = testSupport.kernelToTest();
        tx = beginTransaction( kernel );
        token = tx.token();
        read = tx.dataRead();
        schemaRead = tx.schemaRead();
        cursors = new ManagedTestCursors( tx.cursors() );
    }

    protected Transaction beginTransaction() throws TransactionFailureException
    {
        Kernel kernel = testSupport.kernelToTest();
        return beginTransaction( kernel );
    }

    private static Transaction beginTransaction( Kernel kernel ) throws TransactionFailureException
    {
        return kernel.beginTransaction( Transaction.Type.implicit, LoginContext.AUTH_DISABLED );
    }
}
