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
package org.neo4j.internal.kernel.api;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.LoginContext;

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
public abstract class KernelAPIWriteTestBase<WriteSupport extends KernelAPIWriteTestSupport>
{
    protected static final TemporaryFolder folder = new TemporaryFolder();
    protected static KernelAPIWriteTestSupport testSupport;
    protected Session session;
    protected Modes modes;
    protected static GraphDatabaseService graphDb;

    /**
     * Creates a new instance of WriteSupport, which will be used to execute the concrete test
     */
    public abstract WriteSupport newTestSupport();

    @Before
    public void setupGraph() throws IOException
    {
        if ( testSupport == null )
        {
            folder.create();
            testSupport = newTestSupport();
            testSupport.setup( folder.getRoot() );
            graphDb = testSupport.graphBackdoor();
        }
        testSupport.clearGraph();
        Kernel kernel = testSupport.kernelToTest();
        session = kernel.beginSession( LoginContext.AUTH_DISABLED );
        modes = kernel.modes();
    }

    @After
    public void closeSession()
    {
        session.close();
    }

    @AfterClass
    public static void tearDown()
    {
        if ( testSupport != null )
        {
            testSupport.tearDown();
            folder.delete();
            testSupport = null;
        }
    }
}
