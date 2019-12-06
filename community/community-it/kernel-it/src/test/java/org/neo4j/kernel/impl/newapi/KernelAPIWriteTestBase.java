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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.ResourceLock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.test.extension.ExecutionSharedContext.SHARED_RESOURCE;

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
@SuppressWarnings( "WeakerAccess" )
@TestDirectoryExtension
@ResourceLock( SHARED_RESOURCE )
public abstract class KernelAPIWriteTestBase<WriteSupport extends KernelAPIWriteTestSupport>
{
    protected static KernelAPIWriteTestSupport testSupport;
    protected static GraphDatabaseService graphDb;

    @Inject
    private TestDirectory testDirectory;

    /**
     * Creates a new instance of WriteSupport, which will be used to execute the concrete test
     */
    public abstract WriteSupport newTestSupport();

    @BeforeEach
    public void setupGraph()
    {
        if ( testSupport == null )
        {
            testSupport = newTestSupport();
            testSupport.setup( testDirectory.homeDir() );
            graphDb = testSupport.graphBackdoor();
        }
        testSupport.clearGraph();
    }

    protected KernelTransaction beginTransaction() throws TransactionFailureException
    {
        return beginTransaction( LoginContext.AUTH_DISABLED );
    }

    protected KernelTransaction beginTransaction( LoginContext loginContext ) throws TransactionFailureException
    {
        Kernel kernel = testSupport.kernelToTest();
        return kernel.beginTransaction( KernelTransaction.Type.IMPLICIT, loginContext );
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
}
