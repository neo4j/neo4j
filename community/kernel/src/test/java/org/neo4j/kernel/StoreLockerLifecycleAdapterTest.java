/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.StoreLockerLifecycleAdapter.DATABASE_LOCKED_ERROR_MESSAGE;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TargetDirectory;

public class StoreLockerLifecycleAdapterTest
{
    @Rule public TestName testName = new TestName();
    private String storeDir;
    
    @Before
    public void before()
    {
        storeDir = TargetDirectory.forTest( getClass() ).directory( testName.getMethodName(), true ).getAbsolutePath();
    }

    @Test
    public void shouldAllowDatabasesToUseFilesetsSequentially() throws Exception
    {
        GraphDatabaseService db = null;

        try
        {
            db = new EmbeddedGraphDatabase( storeDir );
        }
        finally
        {
            db.shutdown();
        }

        try
        {
            db = new EmbeddedGraphDatabase( storeDir );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrently() throws Exception
    {
        GraphDatabaseService db = null;

        try
        {
            db = new EmbeddedGraphDatabase( storeDir );

            new EmbeddedGraphDatabase( storeDir );

            fail();
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause().getCause().getMessage(), is( DATABASE_LOCKED_ERROR_MESSAGE ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrentlyEvenIfTheyAreInReadOnlyMode() throws Exception
    {
        GraphDatabaseService db = null;

        try
        {
            db = new EmbeddedGraphDatabase( storeDir );

            new EmbeddedReadOnlyGraphDatabase( storeDir );

            fail();
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause().getCause().getMessage(), is( DATABASE_LOCKED_ERROR_MESSAGE ) );
        }
        finally
        {
            db.shutdown();
        }
    }
}
