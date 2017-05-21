/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertFalse;

public class CreateIndexStressIT
{
    private static final int NUM_PROPS = 400;
    private final AtomicBoolean hasFailed = new AtomicBoolean( false );

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.query_cache_size, "0" );
        }
    };

    private final ExecutorService executorService = Executors.newFixedThreadPool( 10 );

    @After
    public void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    public void shouldHandleConcurrentIndexCreationAndUsage() throws InterruptedException
    {
        // Given
        HashMap<String,Object> params = new HashMap<>();
        params.put( "param", NUM_PROPS );
        db.execute( "FOREACH(x in range(0,$param) | CREATE (:A {prop:x})) ", params );
        db.execute( "CREATE INDEX ON :A(prop) " );

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
                db.execute( query, params ).resultAsString();
            }
            catch ( Exception e )
            {
                hasFailed.set( true );
            }
        } );
    }
}
