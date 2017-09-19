/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;

@SuppressWarnings( "WeakerAccess" )
public abstract class KernelAPIReadTestBase<G extends KernelAPIReadTestSupport>
{
    protected static final TemporaryFolder folder = new TemporaryFolder();
    protected static KernelAPIReadTestSupport testSupport;
    protected static KernelAPI kernel;
    protected CursorFactory cursors;
    protected Transaction read;

    /**
     * Creates a new instance of KernelAPITestSupport, which will be used to execute the concrete test
     */
    public abstract G newTestSupport();

    /**
     * Create the graph which all test in the class will be executed against. The graph is only built once,
     * regardless of the number of tests.
     *
     * @param graphDb a graph API which should be used to build the test graph
     */
    abstract void createTestGraph( GraphDatabaseService graphDb );

    @Before
    public void setupGraph() throws IOException
    {
        if ( testSupport == null )
        {
            folder.create();
            testSupport = newTestSupport();
            testSupport.setup( folder.getRoot(), this::createTestGraph );
            kernel = testSupport.kernelToTest();
        }
        testSupport.beforeEachTest();
        cursors = kernel.cursors();
        read = kernel.beginTransaction();
    }

    @After
    public void closeTransaction() throws Exception
    {
        read.success();
        read.close();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if ( testSupport != null )
        {
            testSupport.tearDown();
            folder.delete();
            testSupport = null;
            kernel = null;
        }
    }
}
