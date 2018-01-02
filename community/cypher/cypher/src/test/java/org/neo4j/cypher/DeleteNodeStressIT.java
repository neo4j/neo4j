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
package org.neo4j.cypher;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.DynamicLabel.label;

public class DeleteNodeStressIT
{

    private final AtomicBoolean hasFailed = new AtomicBoolean( false );

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Before
    public void setup()
    {
        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {

                for ( int j = 0; j < 100; j++ )
                {
                    Node node = db.createNode( label( "L" ) );
                    node.setProperty( "prop", i + j );
                }
                tx.success();
            }
        }
    }

    private final ExecutorService executorService = Executors.newFixedThreadPool( 10 );


    @Test
    public void shouldBeAbleToReturnNodesWhileDeletingNode() throws IOException, ExecutionException, InterruptedException
    {
        // Given
        executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) return n" );
        executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );

        // Then
        assertFalse(hasFailed.get());
    }

    @Test
    public void shouldBeAbleToCheckPropertiesWhileDeletingNode() throws IOException, ExecutionException, InterruptedException
    {
        // Given
        executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) RETURN exists(n.prop)" );
        executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse(hasFailed.get());
    }

    @Test
    public void shouldBeAbleToRemovePropertiesWhileDeletingNode() throws IOException, ExecutionException, InterruptedException
    {
        // Given
        executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) REMOVE n.prop" );
        executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse(hasFailed.get());
    }

    @Test
    public void shouldBeAbleToSetPropertiesWhileDeletingNode() throws IOException, ExecutionException, InterruptedException
    {
        // Given
        executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) SET n.foo = 'bar'" );
        executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse(hasFailed.get());
    }

    private void executeInThread( final String query )
    {
        executorService.execute( new Runnable()
        {
            @Override
            public void run()
            {
                Result execute = db.execute( query );
                try
                {
                    //resultAsString is good test case since it serializes labels, types, properties etc
                    execute.resultAsString();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    hasFailed.set( true );
                }
            }
        } );
    }

}
