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

import org.junit.Test;

public class StoreLockerLifecycleAdapterTest
{
    private static final String DATABASE_NAME_1 = "target/StoreLockerLifecycleAdapterTest/foo";
    private static final String DATABASE_NAME_2 = "target/StoreLockerLifecycleAdapterTest/bar";
    private static final String DATABASE_NAME_3 = "target/StoreLockerLifecycleAdapterTest/baz";

    @Test
    public void shouldAllowDatabasesToUseFilesetsSequentially() throws Exception
    {
        EmbeddedGraphDatabase embeddedGraphDatabase = null;

        try
        {
            embeddedGraphDatabase = new EmbeddedGraphDatabase( DATABASE_NAME_1 );
        }
        finally
        {
            embeddedGraphDatabase.shutdown();
        }

        try
        {
            embeddedGraphDatabase = new EmbeddedGraphDatabase( DATABASE_NAME_1 );
        }
        finally
        {
            embeddedGraphDatabase.shutdown();
        }
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrently() throws Exception
    {
        EmbeddedGraphDatabase embeddedGraphDatabase = null;

        try
        {
            embeddedGraphDatabase = new EmbeddedGraphDatabase( DATABASE_NAME_2 );

            new EmbeddedGraphDatabase( DATABASE_NAME_2 );

            fail();
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getMessage(), is( DATABASE_LOCKED_ERROR_MESSAGE ) );
        }
        finally
        {
            embeddedGraphDatabase.shutdown();
        }
    }

    @Test
    public void shouldNotAllowDatabasesToUseFilesetsConcurrentlyEvenIfTheyAreInReadOnlyMode() throws Exception
    {
        EmbeddedGraphDatabase embeddedGraphDatabase = null;

        try
        {
            embeddedGraphDatabase = new EmbeddedGraphDatabase( DATABASE_NAME_3 );

            new EmbeddedReadOnlyGraphDatabase( DATABASE_NAME_3 );

            fail();
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getMessage(), is( DATABASE_LOCKED_ERROR_MESSAGE ) );
        }
        finally
        {
            embeddedGraphDatabase.shutdown();
        }
    }
}
