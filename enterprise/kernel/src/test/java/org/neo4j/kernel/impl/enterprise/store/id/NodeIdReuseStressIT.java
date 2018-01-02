/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.enterprise.store.id;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseDatabaseRule;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.Race;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class NodeIdReuseStressIT
{
    private static final int CONTESTANTS_COUNT = 12;
    private static final int INITIAL_NODE_COUNT = 10_000;
    private static final int OPERATIONS_COUNT = 10_000;

    @Rule
    public EmbeddedDatabaseRule db = new EnterpriseDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            builder.setConfig( EnterpriseEditionSettings.idTypesToReuse, IdType.NODE.name() );
        }
    };

    @Before
    public void verifyParams() throws Exception
    {
        assertThat( CONTESTANTS_COUNT, greaterThan( 0 ) );
        assertThat( CONTESTANTS_COUNT % 2, equalTo( 0 ) );
        assertThat( INITIAL_NODE_COUNT, greaterThan( 0 ) );
        assertThat( OPERATIONS_COUNT, greaterThan( 1_000 ) );
    }

    @Test
    public void nodeIdsReused() throws Throwable
    {
        createInitialNodes( db );
        long initialHighestNodeId = highestNodeId( db );

        Race race = new Race();

        for ( int i = 0; i < CONTESTANTS_COUNT; i++ )
        {
            if ( i % 2 == 0 )
            {
                race.addContestant( new NodeCreator( db ) );
            }
            else
            {
                race.addContestant( new NodeRemover( db ) );
            }
        }

        race.go();

        int writeContestants = CONTESTANTS_COUNT / 2;
        int createdNodes = writeContestants * OPERATIONS_COUNT;
        long highestNodeIdWithoutReuse = initialHighestNodeId + createdNodes;

        long currentHighestNodeId = highestNodeId( db );

        assertThat( currentHighestNodeId, lessThan( highestNodeIdWithoutReuse ) );

        System.out.println( "highestNodeIdWithoutReuse = " + highestNodeIdWithoutReuse );
        System.out.println( "currentHighestNodeId = " + currentHighestNodeId );
    }

    private static void createInitialNodes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < INITIAL_NODE_COUNT; i++ )
            {
                db.createNode();
            }
            tx.success();
        }
    }

    private static long highestNodeId( GraphDatabaseService db )
    {
        DependencyResolver resolver = dependencyResolver( db );
        NeoStores neoStores = resolver.resolveDependency( NeoStores.class );
        NodeStore nodeStore = neoStores.getNodeStore();
        return nodeStore.getHighestPossibleIdInUse();
    }

    private static void maybeRunIdMaintenance( GraphDatabaseService db, int iteration )
    {
        if ( iteration % 100 == 0 && ThreadLocalRandom.current().nextBoolean() )
        {
            DependencyResolver resolver = dependencyResolver( db );
            resolver.resolveDependency( NeoStoreDataSource.BufferedIdMaintenanceController.class ).maintenance();
        }
    }

    private static DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static class NodeCreator implements Runnable
    {
        final GraphDatabaseService db;

        NodeCreator( GraphDatabaseService db )
        {
            this.db = db;
        }

        @Override
        public void run()
        {
            for ( int i = 0; i < OPERATIONS_COUNT; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode();
                    tx.success();
                }

                maybeRunIdMaintenance( db, i );
            }
        }
    }

    private static class NodeRemover implements Runnable
    {
        final GraphDatabaseService db;

        NodeRemover( GraphDatabaseService db )
        {
            this.db = db;
        }

        @Override
        public void run()
        {
            for ( int i = 0; i < OPERATIONS_COUNT; i++ )
            {
                long highestId = highestNodeId( db );
                if ( highestId > 0 )
                {
                    long id = ThreadLocalRandom.current().nextLong( highestId );

                    try ( Transaction tx = db.beginTx() )
                    {
                        db.getNodeById( id ).delete();
                        tx.success();
                    }
                    catch ( NotFoundException ignore )
                    {
                        // same node was removed concurrently
                    }
                }

                maybeRunIdMaintenance( db, i );
            }
        }
    }
}
