/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.procedure.builtin;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.concurrent.ThreadingExtension;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.GraphDatabaseSettings.cypher_hints_error;
import static org.neo4j.configuration.GraphDatabaseSettings.track_query_cpu_time;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@DbmsExtension( configurationCallback = "configure" )
@ExtendWith( ThreadingExtension.class )
public class ListQueriesProcedureTest
{
    @Inject
    private GraphDatabaseService db;
    @Inject
    private ThreadingRule threads;

    private static final int SECONDS_TIMEOUT = 240;
    private static final Condition<Object> LONG_VALUE = new Condition<>( value -> value instanceof Long, "long value" );

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( cypher_hints_error, true )
               .setConfig( GraphDatabaseSettings.track_query_allocation, true )
               .setConfig( track_query_cpu_time, true );
    }

    @Test
    void shouldContainTheQueryItself()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // given
            String query = "CALL dbms.listQueries";

            // when
            Result result = transaction.execute( query );

            // then
            Map<String,Object> row = result.next();
            assertFalse( result.hasNext() );
            assertEquals( query, row.get( "query" ) );
            assertEquals( db.databaseName(), row.get( "database" ) );
            transaction.commit();
        }
    }

    @Test
    void shouldNotIncludeDeprecatedFields()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // when
            Result result = transaction.execute( "CALL dbms.listQueries" );

            // then
            Map<String,Object> row = result.next();
            assertThat( row ).doesNotContainKey( "elapsedTime" );
            assertThat( row ).doesNotContainKey( "connectionDetails" );
            transaction.commit();
        }
    }

    @Test
    void shouldContainSpecificConnectionDetails()
    {
        // when
        Map<String,Object> data = getQueryListing( "CALL dbms.listQueries" );

        // then
        assertThat( data ).containsKey( "protocol" );
        assertThat( data ).containsKey( "connectionId" );
        assertThat( data ).containsKey( "clientAddress" );
        assertThat( data ).containsKey( "requestUri" );
    }

    @Test
    void shouldContainPageHitsAndPageFaults() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        try ( Resource<Node> test = test( Transaction::createNode, query ) )
        {
            // when
            Map<String,Object> data = getQueryListing( query );

            // then
            assertThat( data ).hasEntrySatisfying( "pageHits", LONG_VALUE );
            assertThat( data ).hasEntrySatisfying( "pageFaults", LONG_VALUE );
        }
    }

    @Test
    void shouldListUsedIndexes() throws Exception
    {
        // given
        String label = "IndexedLabel";
        String property = "indexedProperty";
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label( label ) ).on( property ).create();
            tx.commit();
        }
        ensureIndexesAreOnline();
        shouldListUsedIndexes( label, property );
    }

    private void ensureIndexesAreOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( SECONDS_TIMEOUT, SECONDS );
            tx.commit();
        }
    }

    @Test
    void shouldListUsedUniqueIndexes() throws Exception
    {
        // given
        String label = "UniqueLabel";
        String property = "uniqueProperty";
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label( label ) ).assertPropertyIsUnique( property ).create();
            tx.commit();
        }
        ensureIndexesAreOnline();
        shouldListUsedIndexes( label, property );
    }

    @Test
    void shouldListIndexesUsedForScans() throws Exception
    {
        // given
        final String QUERY = "MATCH (n:Node) USING INDEX n:Node(value) WHERE 1 < n.value < 10 SET n.value = 2";
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label( "Node" ) ).on( "value" ).create();
            tx.commit();
        }
        ensureIndexesAreOnline();
        try ( Resource<Node> test = test( tx ->
        {
            Node node = tx.createNode( label( "Node" ) );
            node.setProperty( "value", 5L );
            return node;
        }, QUERY ) )
        {
            // when
            Map<String,Object> data = getQueryListing( QUERY );

            // then
            assertThat( data ).hasEntrySatisfying( "indexes", value -> assertThat( value ).isInstanceOf( List.class ) );
            @SuppressWarnings( "unchecked" )
            List<Map<String,Object>> indexes = (List<Map<String,Object>>) data.get( "indexes" );
            assertEquals( 1, indexes.size(), "number of indexes used" );
            Map<String,Object> index = indexes.get( 0 );
            assertThat( index ).containsEntry( "identifier", "n" );
            assertThat( index ).containsEntry( "label", "Node" );
            assertThat( index ).containsEntry( "propertyKey", "value" );
        }
    }

    private void shouldListUsedIndexes( String label, String property ) throws Exception
    {
        // given
        final String QUERY1 = "MATCH (n:" + label + "{" + property + ":5}) USING INDEX n:" + label + "(" + property +
                ") SET n." + property + " = 3";
        try ( Resource<Node> test = test( tx ->
        {
            Node node = tx.createNode( label( label ) );
            node.setProperty( property, 5L );
            return node;
        }, QUERY1 ) )
        {
            // when
            Map<String,Object> data = getQueryListing( QUERY1 );

            // then
            assertThat( data ).containsEntry( "runtime", "interpreted" );
            assertThat( data ).containsEntry( "status", "waiting" );
            assertThat( data ).hasEntrySatisfying( "indexes", value -> assertThat( value ).isInstanceOf( List.class ) );
            @SuppressWarnings( "unchecked" )
            List<Map<String,Object>> indexes = (List<Map<String,Object>>) data.get( "indexes" );
            assertEquals( 1, indexes.size(), "number of indexes used" );
            Map<String,Object> index = indexes.get( 0 );
            assertThat( index ).containsEntry( "identifier", "n" );
            assertThat( index ).containsEntry( "label", label );
            assertThat( index ).containsEntry( "propertyKey", property );
        }

        // given
        final String QUERY2 = "MATCH (n:" + label + "{" + property + ":3}) USING INDEX n:" + label + "(" + property +
                ") MATCH (u:" + label + "{" + property + ":4}) USING INDEX u:" + label + "(" + property +
                ") CREATE (n)-[:KNOWS]->(u)";
        try ( Resource<Node> test = test( tx ->
        {
            Node node = tx.createNode( label( label ) );
            node.setProperty( property, 4L );
            return node;
        }, QUERY2 ) )
        {
            // when
            Map<String,Object> data = getQueryListing( QUERY2 );

            // then
            assertThat( data ).hasEntrySatisfying( "indexes", value -> assertThat( value ).isInstanceOf( List.class ) );
            @SuppressWarnings( "unchecked" )
            List<Map<String,Object>> indexes = (List<Map<String,Object>>) data.get( "indexes" );
            assertEquals( 2, indexes.size(), "number of indexes used" );

            Map<String,Object> index1 = indexes.get( 0 );
            assertThat( index1 ).containsEntry( "identifier", "n" );
            assertThat( index1 ).containsEntry( "label", label );
            assertThat( index1 ).containsEntry( "propertyKey", property );

            Map<String,Object> index2 = indexes.get( 1 );
            assertThat( index2 ).containsEntry( "identifier", "u" );
            assertThat( index2 ).containsEntry( "label", label );
            assertThat( index2 ).containsEntry( "propertyKey", property );
        }
    }

    private Map<String,Object> getQueryListing( String query )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            try ( Result rows = transaction.execute( "CALL dbms.listQueries" ) )
            {
                while ( rows.hasNext() )
                {
                    Map<String,Object> row = rows.next();
                    if ( query.equals( row.get( "query" ) ) )
                    {
                        return row;
                    }
                }
            }
            transaction.commit();
        }
        throw new AssertionError( "query not active: " + query );
    }

    private static class Resource<T> implements AutoCloseable
    {
        private final CountDownLatch latch;
        private final CountDownLatch finishLatch;
        private final T resource;

        private Resource( CountDownLatch latch, CountDownLatch finishLatch, T resource )
        {
            this.latch = latch;
            this.finishLatch = finishLatch;
            this.resource = resource;
        }

        @Override
        public void close() throws InterruptedException
        {
            latch.countDown();
            finishLatch.await();
        }

        public T resource()
        {
            return resource;
        }
    }

    private <T extends Entity> Resource<T> test( Function<Transaction, T> setup, String... queries )
            throws InterruptedException, ExecutionException
    {
        CountDownLatch resourceLocked = new CountDownLatch( 1 );
        CountDownLatch listQueriesLatch = new CountDownLatch( 1 );
        CountDownLatch finishQueriesLatch = new CountDownLatch( 1 );
        T resource;
        try ( Transaction tx = db.beginTx() )
        {
            resource = setup.apply(tx);
            tx.commit();
        }
        threads.execute( parameter ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.acquireWriteLock( resource );
                resourceLocked.countDown();
                listQueriesLatch.await();
            }
            return null;
        }, null );
        resourceLocked.await();

        threads.executeAndAwait( parameter ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( String query : queries )
                {
                    tx.execute( query ).close();
                }
                tx.commit();
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( t );
            }
            finally
            {
                finishQueriesLatch.countDown();
            }
            return null;
        }, null, waitingWhileIn( Locks.Client.class, "acquireExclusive" ), SECONDS_TIMEOUT, SECONDS );

        return new Resource<>( listQueriesLatch, finishQueriesLatch, resource );
    }
}
