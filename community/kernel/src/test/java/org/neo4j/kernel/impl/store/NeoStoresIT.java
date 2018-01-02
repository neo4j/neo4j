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
package org.neo4j.kernel.impl.store;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

public class NeoStoresIT
{
    public final @Rule DatabaseRule db = new EmbeddedDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            builder.setConfig(  GraphDatabaseSettings.dense_node_threshold, "1");
        }
    };

    private static final DynamicRelationshipType FRIEND = DynamicRelationshipType.withName( "FRIEND" );

    private static final String LONG_STRING_VALUE =
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALA"
            +
            "ALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALALONG!!";


    @Test
    public void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord()
            throws InterruptedException
    {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        final long[] latestNodeId = new long[1];

        for ( int i = 0; i < 100_000; i++ )
        {
            executor.scheduleAtFixedRate( new Runnable()
            {
                @Override
                public void run()
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        Node node = db.createNode();
                        latestNodeId[0] = node.getId();
                        node.setProperty( "largeProperty", LONG_STRING_VALUE );
                        tx.success();
                    }
                }
            }, 5, 25, TimeUnit.MILLISECONDS );
        }

        for ( int i = 0; i < 100_000; i++ )
        {
            try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
            {
                Node node = db.getGraphDatabaseService().getNodeById( latestNodeId[0] );

                for ( String propertyKey : node.getPropertyKeys() )
                {
                    node.getProperty( propertyKey );
                }
                tx.success();
            }
            catch ( NotFoundException e )
            {
                // This will catch nodes not found (expected) and also PropertyRecords not found (shouldn't happen
                // but handled in shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord)
            }
        }

        executor.shutdown();
        executor.awaitTermination( 2000, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord()
            throws InterruptedException
    {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        final long[] latestNodeId = new long[1];

        for ( int i = 0; i < 100_000; i++ )
        {
            executor.scheduleAtFixedRate( new Runnable()
            {
                @Override
                public void run()
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        Node node = db.createNode();
                        latestNodeId[0] = node.getId();
                        node.setProperty( "largeProperty", LONG_STRING_VALUE );
                        tx.success();
                    }
                }
            }, 5, 25, TimeUnit.MILLISECONDS );
        }

        for ( int i = 0; i < 100_000; i++ )
        {
            try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
            {
                Node node = db.getGraphDatabaseService().getNodeById( latestNodeId[0] );

                for ( String propertyKey : node.getPropertyKeys() )
                {
                    node.getProperty( propertyKey );
                }
                tx.success();
            }
            catch ( NotFoundException e )
            {
                if ( Exceptions.contains( e, InvalidRecordException.class ) )
                {
                    throw e;
                }
            }
        }

        executor.shutdown();
        executor.awaitTermination( 2000, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldWriteOutThePropertyRecordBeforeReferencingItFromARelationshipRecord()
            throws InterruptedException
    {
        final long node1Id;
        final long node2Id;
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1Id = node1.getId();

            Node node2 = db.createNode();
            node2Id = node2.getId();

            tx.success();
        }

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        final long[] latestRelationshipId = new long[1];

        for ( int i = 0; i < 100_000; i++ )
        {
            executor.scheduleAtFixedRate( new Runnable()
            {
                @Override
                public void run()
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        Node node1 = db.getGraphDatabaseService().getNodeById( node1Id );
                        Node node2 = db.getGraphDatabaseService().getNodeById( node2Id );

                        Relationship rel = node1.createRelationshipTo( node2, FRIEND );
                        latestRelationshipId[0] = rel.getId();
                        rel.setProperty( "largeProperty", LONG_STRING_VALUE );

                        tx.success();
                    }
                }
            }, 5, 25, TimeUnit.MILLISECONDS );
        }

        for ( int i = 0; i < 100_000; i++ )
        {
            try ( Transaction tx = db.getGraphDatabaseAPI().beginTx() )
            {
                Relationship rel = db.getGraphDatabaseService().getRelationshipById( latestRelationshipId[0] );

                for ( String propertyKey : rel.getPropertyKeys() )
                {
                    rel.getProperty( propertyKey );
                }
                tx.success();
            }
            catch ( NotFoundException e )
            {
                if ( Exceptions.contains( e, InvalidRecordException.class ) )
                {
                    throw e;
                }
            }
        }

        executor.shutdown();
        executor.awaitTermination( 2000, TimeUnit.MILLISECONDS );
    }
}
