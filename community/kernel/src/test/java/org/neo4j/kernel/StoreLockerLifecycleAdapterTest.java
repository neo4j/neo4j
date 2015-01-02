/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class StoreLockerLifecycleAdapterTest
{
    @Rule public TestName testName = new TestName();
    private String storeDir;
    
    @Before
    public void before()
    {
        storeDir = TargetDirectory.forTest( getClass() ).cleanDirectory( testName.getMethodName() ).getAbsolutePath();
    }

    @Test
    public void shouldAllowDatabasesToUseFilesetsSequentially() throws Exception
    {
        new GraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();
        new GraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrently() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );

            fail();
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause().getCause(), instanceOf( StoreLockException.class ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrentlyEvenIfTheyAreInReadOnlyMode() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir).
                    setConfig( GraphDatabaseSettings.read_only, Settings.TRUE ).
                    newGraphDatabase();

            fail();
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getCause().getCause(), instanceOf( StoreLockException.class ) );
        }
        finally
        {
            db.shutdown();
        }
    }
}
