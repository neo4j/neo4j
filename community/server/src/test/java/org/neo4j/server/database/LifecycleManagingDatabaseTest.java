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
package org.neo4j.server.database;

import org.junit.Test;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LifecycleManagingDatabaseTest
{
    @Test
    public void mustIgnoreExceptionsFromPreLoadingCypherQuery() throws Throwable
    {
        // Given a lifecycled database that'll try to warm up Cypher when it starts
        final GraphDatabaseAPI mockDb = mock( GraphDatabaseAPI.class );
        Config config = new Config();
        GraphDatabaseFacadeFactory.Dependencies deps =
                GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
        LifecycleManagingDatabase.GraphFactory factory = new LifecycleManagingDatabase.GraphFactory()
        {
            @Override
            public GraphDatabaseAPI newGraphDatabase( Config config,
                                                      GraphDatabaseFacadeFactory.Dependencies dependencies )
            {
                return mockDb;
            }
        };
        LifecycleManagingDatabase db = new LifecycleManagingDatabase( config, factory, deps )
        {
            @Override
            protected boolean isInTestMode()
            {
                return false;
            }
        };

        // When the execution of the query fails (for instance when this is a slave that just joined a cluster and is
        // working on catching up to the master)
        when( mockDb.execute( LifecycleManagingDatabase.CYPHER_WARMUP_QUERY ) ).thenThrow(
                new TransactionFailureException( "Boo" ) );

        // Then the database should still start up as normal, without bubbling the exception up
        db.init();
        db.start();
        assertTrue( "the database should be running", db.isRunning() );
        db.stop();
        db.shutdown();
    }
}
