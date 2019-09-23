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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
class CreateIndexStressIT
{
    private static final int NUM_PROPS = 400;
    private final AtomicBoolean hasFailed = new AtomicBoolean( false );

    @Inject
    private GraphDatabaseService db;

    @ExtensionCallback
    static void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.query_cache_size, 0 );
    }

    private final ExecutorService executorService = Executors.newFixedThreadPool( 10 );

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    void shouldHandleConcurrentIndexCreationAndUsage() throws InterruptedException
    {
        // Given
        HashMap<String,Object> params = new HashMap<>();
        params.put( "param", NUM_PROPS );
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( "FOREACH(x in range(0,$param) | CREATE (:A {prop:x})) ", params );
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( "CREATE INDEX FOR (n:A) ON (n.prop) " );
            transaction.commit();
        }

        // When
        for ( int i = 0; i < NUM_PROPS; i++ )
        {
            params.put( "param", i );
            executeInThread( "MATCH (n:A) WHERE n.prop CONTAINS 'A' RETURN n.prop", params );
        }

        // Then
        awaitAndAssertNoErrors();
    }

    private void awaitAndAssertNoErrors() throws InterruptedException
    {
        executorService.awaitTermination( 3L, TimeUnit.SECONDS );
        assertFalse( hasFailed.get() );
    }

    private void executeInThread( final String query, Map<String,Object> params )
    {
        executorService.execute( () ->
        {
            try
            {
                try ( Transaction transaction = db.beginTx() )
                {
                    transaction.execute( query, params ).resultAsString();
                    transaction.commit();
                }
            }
            catch ( Exception e )
            {
                hasFailed.set( true );
            }
        } );
    }
}
