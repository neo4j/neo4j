/*
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.embedded.GraphDatabase;
import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
        shouldNotAllowDatabasesToUseFilesetsConcurrently( Consumers.<TestGraphDatabase.Builder>noop() );
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrentlyEvenIfTheyAreInReadOnlyMode() throws Exception
    {
        shouldNotAllowDatabasesToUseFilesetsConcurrently( new Consumer<TestGraphDatabase.Builder>()
        {
            @Override
            public void accept( TestGraphDatabase.Builder builder )
            {
                builder.readOnly();
            }
        } );
    }

    private void shouldNotAllowDatabasesToUseFilesetsConcurrently( Consumer<TestGraphDatabase.Builder> configure ) throws Exception
    {
        GraphDatabase db = newDb();
        try
        {
            TestGraphDatabase.Builder builder = TestGraphDatabase.build();
            configure.accept( builder );
            builder.open( storeDir() );
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

    private GraphDatabase newDb()
    {
        return TestGraphDatabase.open( storeDir() );
    }

    private File storeDir()
    {
        return directory.graphDbDir();
    }
}
