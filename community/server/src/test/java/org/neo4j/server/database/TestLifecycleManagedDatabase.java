/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.database;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.test.Mute;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerTestUtils.createTempDir;
import static org.neo4j.test.Mute.muteAll;

public class TestLifecycleManagedDatabase
{
    @Rule
    public Mute mute = muteAll();
    private File databaseDirectory;
    private Database theDatabase;
    private boolean deletionFailureOk;
    private LifecycleManagingDatabase.GraphFactory dbFactory;

    @Before
    public void setup() throws Exception
    {
        databaseDirectory = createTempDir();

        dbFactory = mock( LifecycleManagingDatabase.GraphFactory.class );
        when(dbFactory.newGraphDatabase( any( Config.class ), any(Function.class), any( Iterable.class ),
                any( Iterable.class ), any( Iterable.class ) )).thenReturn( mock( GraphDatabaseAPI.class) );
        theDatabase = newDatabase();
    }

    private LifecycleManagingDatabase newDatabase()
    {
        Config dbConfig = new Config(stringMap( GraphDatabaseSettings.store_dir.name(), databaseDirectory.getAbsolutePath() ));
        return new LifecycleManagingDatabase( dbConfig, dbFactory, Iterables.<KernelExtensionFactory<?>>empty() );
    }

    @After
    public void shutdownDatabase() throws Throwable
    {
        this.theDatabase.stop();

        try
        {
            FileUtils.forceDelete( databaseDirectory );
        }
        catch ( IOException e )
        {
            // TODO Removed this when EmbeddedGraphDatabase startup failures
            // closes its
            // files properly.
            if ( !deletionFailureOk )
            {
                throw e;
            }
        }
    }

    @Test
    public void shouldLogOnSuccessfulStartup() throws Throwable
    {
        InMemoryAppender appender = new InMemoryAppender( LifecycleManagingDatabase.log );

        theDatabase.start();

        assertThat( appender.toString(), containsString( "Successfully started database" ) );
    }

    @Test
    public void shouldShutdownCleanly() throws Throwable
    {
        InMemoryAppender appender = new InMemoryAppender( LifecycleManagingDatabase.log );

        theDatabase.start();
        theDatabase.stop();

        assertThat( appender.toString(), containsString( "Successfully stopped database" ) );
    }

    @Test
    public void shouldComplainIfDatabaseLocationIsAlreadyInUse() throws Throwable
    {
        deletionFailureOk = true;
        theDatabase.start();

        LifecycleManagingDatabase db = newDatabase();

        try
        {
            db.start();
        }
        catch ( RuntimeException e )
        {
            // Wrapped in a lifecycle exception, needs to be dug out
            assertThat( e.getCause().getCause(), instanceOf( StoreLockException.class ) );
        }
    }

    @Test
    public void shouldBeAbleToGetLocation() throws Throwable
    {
        theDatabase.start();
        assertThat( theDatabase.getLocation(), is( databaseDirectory.getAbsolutePath() ) );
    }
}
