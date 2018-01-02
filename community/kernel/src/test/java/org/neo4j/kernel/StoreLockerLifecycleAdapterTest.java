/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class StoreLockerLifecycleAdapterTest
{
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldAllowDatabasesToUseFilesetsSequentially() throws Exception
    {
        newDb().shutdown();
        newDb().shutdown();
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrently() throws Exception
    {
        shouldNotAllowDatabasesToUseFilesetsConcurrently( stringMap() );
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrentlyEvenIfTheyAreInReadOnlyMode() throws Exception
    {
        shouldNotAllowDatabasesToUseFilesetsConcurrently(
                stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
    }

    private void shouldNotAllowDatabasesToUseFilesetsConcurrently( Map<String,String> config ) throws Exception
    {
        GraphDatabaseService db = newDb();
        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir() )
                    .setConfig( config ).newGraphDatabase();

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

    private GraphDatabaseService newDb()
    {
        return new GraphDatabaseFactory().newEmbeddedDatabase( storeDir() );
    }

    private String storeDir()
    {
        return directory.absolutePath();
    }
}
