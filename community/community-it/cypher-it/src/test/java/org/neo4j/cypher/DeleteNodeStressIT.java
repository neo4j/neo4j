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
package org.neo4j.cypher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.neo4j.graphdb.Label.label;

@ImpermanentDbmsExtension
class DeleteNodeStressIT
{
    private final ExecutorService executorService = Executors.newFixedThreadPool( 10 );

    @Inject
    private GraphDatabaseService db;

    @BeforeEach
    void setup()
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
                tx.commit();
            }
        }
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    void shouldBeAbleToReturnNodesWhileDeletingNode() throws InterruptedException, ExecutionException
    {
        // Given
        Future query1 = executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) return n" );
        Future query2 = executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // Then
        query1.get();
        query2.get();
    }

    @Test
    void shouldBeAbleToCheckPropertiesWhileDeletingNode() throws InterruptedException, ExecutionException
    {
        // Given
        Future query1 = executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) RETURN exists(n.prop)" );
        Future query2 = executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        query1.get();
        query2.get();
    }

    @Test
    void shouldBeAbleToRemovePropertiesWhileDeletingNode() throws InterruptedException, ExecutionException
    {
        // Given
        Future query1 = executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) REMOVE n.prop" );
        Future query2 = executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        query1.get();
        query2.get();
    }

    @Test
    void shouldBeAbleToSetPropertiesWhileDeletingNode() throws InterruptedException, ExecutionException
    {
        // Given
        Future query1 = executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n MATCH (n) SET n.foo = 'bar'" );
        Future query2 = executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        query1.get();
        query2.get();
    }

    @Test
    void shouldBeAbleToCheckLabelsWhileDeleting() throws InterruptedException, ExecutionException
    {
        // Given
        Future query1 = executeInThread( "MATCH (n:L {prop:42}) OPTIONAL MATCH (m:L {prop:1337}) WITH n RETURN labels(n)" );
        Future query2 = executeInThread( "MATCH (n:L {prop:42}) DELETE n" );

        // When
        query1.get();
        query2.get();
    }

    private Future executeInThread( final String query )
    {
        return executorService.submit( () ->
        {
            try ( Transaction transaction = db.beginTx() )
            {
                db.execute( query ).resultAsString();
                transaction.commit();
            }
        } );
    }
}
