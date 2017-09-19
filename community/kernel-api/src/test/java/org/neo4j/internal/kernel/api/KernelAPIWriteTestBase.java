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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;

@SuppressWarnings( "WeakerAccess" )
public abstract class KernelAPIWriteTestBase<G extends KernelAPIWriteTestSupport>
{
    protected static final TemporaryFolder folder = new TemporaryFolder();
    protected static KernelAPIWriteTestSupport testSupport;
    protected static KernelAPI kernel;
    protected static GraphDatabaseService graphDb;

    /**
     * Creates a new instance of KernelAPITestSupport, which will be used to execute the concrete test
     */
    public abstract G newTestSupport();

    @Before
    public void setupGraph() throws IOException
    {
        if ( testSupport == null )
        {
            folder.create();
            testSupport = newTestSupport();
            testSupport.setup( folder.getRoot() );
            kernel = testSupport.kernelToTest();
            graphDb = testSupport.graphBackdoor();
        }
        testSupport.beforeEachTest();
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
