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
package org.neo4j.kernel.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Map;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@ExtendWith( TestDirectoryExtension.class )
public class StoreLockerLifecycleAdapterTest
{
    @Resource
    public TestDirectory directory;

    @Test
    public void shouldAllowDatabasesToUseFilesetsSequentially()
    {
        newDb().shutdown();
        newDb().shutdown();
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrently()
    {
        shouldNotAllowDatabasesToUseFilesetsConcurrently( stringMap() );
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrentlyEvenIfTheyAreInReadOnlyMode()
    {
        shouldNotAllowDatabasesToUseFilesetsConcurrently(
                stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
    }

    private void shouldNotAllowDatabasesToUseFilesetsConcurrently( Map<String,String> config )
    {
        GraphDatabaseService db = newDb();
        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir() )
                    .setConfig( config ).newGraphDatabase();

            fail("Failure was expected");
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
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir() );
    }

    private File storeDir()
    {
        return directory.absolutePath();
    }
}
