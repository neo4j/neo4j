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
package org.neo4j.server.database;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.ServerTestUtils.createTempDir;

@ExtendWith( SuppressOutputExtension.class )
@ImpermanentDbmsExtension( configurationCallback = "configure" )
@ResourceLock( Resources.SYSTEM_OUT )
class TestLifecycleManagedDatabase
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private DatabaseManagementService dbms;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setInternalLogProvider( logProvider );
    }

    private File dataDirectory;
    private DatabaseService theDatabase;
    private boolean deletionFailureOk;
    private GraphFactory dbFactory;
    private Config dbConfig;

    @BeforeEach
    void setup() throws Exception
    {
        dataDirectory = createTempDir();

        dbFactory = new SimpleGraphFactory( dbms );
        dbConfig = Config.defaults( GraphDatabaseSettings.data_directory, dataDirectory.toPath().toAbsolutePath() );
        theDatabase = newDatabase();
    }

    private LifecycleManagingDatabaseService newDatabase()
    {
        return new LifecycleManagingDatabaseService( dbConfig, dbFactory,
                GraphDatabaseDependencies.newDependencies().userLogProvider( logProvider ) );
    }

    @AfterEach
    void shutdownDatabase() throws Throwable
    {
        this.theDatabase.stop();

        try
        {
            FileUtils.forceDelete( dataDirectory );
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
    void shouldLogOnSuccessfulStartup() throws Throwable
    {
        theDatabase.start();

        logProvider.assertAtLeastOnce(
                inLog( LifecycleManagingDatabaseService.class ).info( "Started." )
        );
    }

    @Test
    void shouldShutdownCleanly() throws Throwable
    {
        theDatabase.start();
        theDatabase.stop();

        logProvider.assertAtLeastOnce(
                inLog( LifecycleManagingDatabaseService.class ).info( "Stopped." )
        );
    }

    @Test
    void shouldComplainIfDatabaseLocationIsAlreadyInUse() throws Throwable
    {
        deletionFailureOk = true;
        theDatabase.start();

        LifecycleManagingDatabaseService db = newDatabase();

        try
        {
            db.start();
        }
        catch ( RuntimeException e )
        {
            // Wrapped in a lifecycle exception, needs to be dug out
            assertThat( e.getCause().getCause(), instanceOf( FileLockException.class ) );
        }
    }
}
