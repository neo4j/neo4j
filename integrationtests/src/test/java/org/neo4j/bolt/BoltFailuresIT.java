/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class BoltFailuresIT
{
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();

    private GraphDatabaseService db;

    @After
    public void shutdownDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test( timeout = 20_000 )
    public void throwsWhenSessionCreationFails()
    {
        WorkerFactory workerFactory = mock( WorkerFactory.class );
        when( workerFactory.newWorker( anyString(), any() ) ).thenThrow( new IllegalStateException( "Oh!" ) );

        db = newDbFactory( new BoltKernelExtensionWithWorkerFactory( workerFactory ) );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost" ) )
        {
            driver.session();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ConnectionFailureException.class ) );
        }
    }

    private GraphDatabaseService newDbFactory( BoltKernelExtension boltKernelExtension )
    {
        return new GraphDatabaseFactoryWithCustomBoltKernelExtension( boltKernelExtension )
                .newEmbeddedDatabaseBuilder( dir.graphDbDir() )
                .setConfig( boltConnector( "0" ).type, BOLT.name() )
                .setConfig( boltConnector( "0" ).enabled, TRUE )
                .setConfig( GraphDatabaseSettings.auth_enabled, FALSE )
                .newGraphDatabase();
    }

    private static class BoltKernelExtensionWithWorkerFactory extends BoltKernelExtension
    {
        final WorkerFactory workerFactory;

        BoltKernelExtensionWithWorkerFactory( WorkerFactory workerFactory )
        {
            this.workerFactory = workerFactory;
        }

        @Override
        protected WorkerFactory createWorkerFactory( BoltFactory boltFactory, JobScheduler scheduler,
                Dependencies dependencies, LogService logService )
        {
            return workerFactory;
        }
    }
}
