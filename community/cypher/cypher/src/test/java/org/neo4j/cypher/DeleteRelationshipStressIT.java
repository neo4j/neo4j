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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Resource;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.graphdb.Label.label;

@ExtendWith( ImpermanentDatabaseExtension.class )
class DeleteRelationshipStressIT
{
    private final AtomicBoolean hasFailed = new AtomicBoolean( false );
    private final ExecutorService executorService = Executors.newFixedThreadPool( 10 );

    @Resource
    private ImpermanentDatabaseRule db;

    @BeforeEach
    void setup()
    {
        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {

                Node prev = null;
                for ( int j = 0; j < 100; j++ )
                {
                    Node node = db.createNode( label( "L" ) );

                    if ( prev != null )
                    {
                        Relationship rel = prev.createRelationshipTo( node, RelationshipType.withName( "T" ) );
                        rel.setProperty( "prop", i + j );
                    }
                    prev = node;
                }
                tx.success();
            }
        }
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    void shouldBeAbleToReturnRelsWhileDeletingRelationship() throws InterruptedException
    {
        // Given
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) OPTIONAL MATCH (:L)-[:T {prop:1337}]-(:L) WITH r MATCH ()-[r]-() return r" );
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) DELETE r" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );

        // Then
        assertFalse(hasFailed.get());
    }

    @Test
    void shouldBeAbleToGetPropertyWhileDeletingRelationship() throws InterruptedException
    {
        // Given
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) OPTIONAL MATCH (:L)-[:T {prop:1337}]-(:L) WITH r MATCH ()-[r]-() return r.prop" );
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) DELETE r" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse(hasFailed.get());
    }

    @Test
    void shouldBeAbleToCheckPropertiesWhileDeletingRelationship() throws InterruptedException
    {
        // Given
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) " +
                "OPTIONAL MATCH (:L)-[:T {prop:1337}]-(:L) WITH r MATCH ()-[r]-() return exists(r.prop)" );
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) DELETE r" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse(hasFailed.get());
    }

    @Test
    void shouldBeAbleToRemovePropertiesWhileDeletingRelationship() throws InterruptedException
    {
        // Given
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) " +
                "OPTIONAL MATCH (:L)-[:T {prop:1337}]-(:L) WITH r MATCH ()-[r]-() REMOVE r.prop" );
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) DELETE r" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse(hasFailed.get());
    }

    @Test
    void shouldBeAbleToSetPropertiesWhileDeletingRelationship() throws InterruptedException
    {
        // Given
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) " +
                "OPTIONAL MATCH (:L)-[:T {prop:1337}]-(:L) WITH r MATCH ()-[r]-() SET r.foo = 'bar'" );
        executeInThread( "MATCH (:L)-[r:T {prop:42}]-(:L) DELETE r" );

        // When
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse(hasFailed.get());
    }

    private void executeInThread( final String query )
    {
        executorService.execute( () ->
        {
            Result execute = db.execute( query );
            try
            {
                //resultAsString is good test case since it serializes labels, types, properties etc
                execute.resultAsString();
            }
            catch ( Exception e )
            {
                hasFailed.set( true );
            }
        } );
    }
}
