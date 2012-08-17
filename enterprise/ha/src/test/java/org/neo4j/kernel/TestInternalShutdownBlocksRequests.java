/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.ha.DatabaseNotRunningException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.MasterGraphDatabase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestInternalShutdownBlocksRequests
{

    private static LocalhostZooKeeperCluster zoo;
    private final TargetDirectory dir = forTest( getClass() );

    @BeforeClass
    public static void startZoo() throws Exception
    {
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
    }

    @Test
    public void shouldBlockEvenIfInternalDatabaseIsInTheMiddleOfShuttingDown() throws Exception
    {
        final HighlyAvailableGraphDatabase db = start( dir.directory( "master", true ).getAbsolutePath(), 1,
                zoo.getConnectionString() );

        // Hook into the internal db's lifecycle, and inject verification code that we want to run just as the internal
        // database is done shutting down.
        final LifeSupport masterDbLife = db.getDependencyResolver().resolveDependency( MasterGraphDatabase.class ).life;
        masterDbLife.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
            {
                if ( instance instanceof XaDataSourceManager && to == LifecycleStatus.SHUTDOWN)
                {
                    // Here we are.

                    // Now, we make sure that operations on the HighlyAvailableGraphDatabase block properly.
                    // Because we have set HaSettings.read_timeout to 5 seconds (minimum allowed) when we created the database,
                    // this call should fail after that timeout with an appropriate exception.

                    try
                    {
                        db.getNodeById( 0 );
                    } catch(DatabaseNotRunningException e)
                    {
                        // Yay!
                        return;
                    }

                    fail("Expected db.getNodeById() to throw exception, since the database is shut down.");
                }
            }
        } );

        // Do internal shutdown, triggering the lifecycle listener above
        db.internalShutdown( false );

    }

    private HighlyAvailableGraphDatabase start( String storeDir, int i, String zkConnectString )
    {
        return new HighlyAvailableGraphDatabase( storeDir, stringMap(
                HaConfig.CONFIG_KEY_SERVER_ID, "" + i,
                HaConfig.CONFIG_KEY_SERVER, "localhost:" + ( 6666 + i ),
                HaConfig.CONFIG_KEY_COORDINATORS, zkConnectString,
                HaConfig.CONFIG_KEY_PULL_INTERVAL, 0 + "ms",
                HaSettings.read_timeout.name(), "5"/* seconds */));
    }
}
