/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.enterprise.builtinprocs;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentEnterpriseDatabaseRule;
import org.neo4j.test.rule.VerboseTimeout;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_hints_error;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.track_query_allocation;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.track_query_cpu_time;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

public class ListQueriesProcedureTest
{
    private final DatabaseRule db = new ImpermanentEnterpriseDatabaseRule()
            .withSetting( cypher_hints_error, Settings.TRUE )
            .withSetting( GraphDatabaseSettings.track_query_allocation, Settings.TRUE )
            .withSetting( track_query_cpu_time, Settings.TRUE )
            .startLazily();

    private final ThreadingRule threads = new ThreadingRule();

    @Rule
    public final RuleChain chain = RuleChain.outerRule( db ).around( threads );

    private static final int SECONDS_TIMEOUT = 240;

    @Rule
    public VerboseTimeout timeout = VerboseTimeout.builder().withTimeout( SECONDS_TIMEOUT - 2, TimeUnit.SECONDS ).build();

    @Test
    public void shouldContainTheQueryItself()
    {
        // given
        String query = "CALL dbms.listQueries";

        // when
        Result result = db.execute( query );

        // then
        Map<String,Object> row = result.next();
        assertFalse( result.hasNext() );
        assertEquals( query, row.get( "query" ) );
    }

    @Test
    public void shouldNotIncludeDeprecatedFields()
    {
        // when
        Result result = db.execute( "CALL dbms.listQueries" );

        // then
        Map<String,Object> row = result.next();
        assertThat( row, not( hasKey( "elapsedTime" ) ) );
        assertThat( row, not( hasKey( "connectionDetails" ) ) );
    }

    @Test
    public void shouldProvideElapsedCpuTimePlannerConnectionDetailsPageHitsAndFaults() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            // when
            Map<String,Object> data = getQueryListing( query );

            // then
            assertThat( data, hasKey( "elapsedTimeMillis" ) );
            Object elapsedTime = data.get( "elapsedTimeMillis" );
            assertThat( elapsedTime, instanceOf( Long.class ) );
            assertThat( data, hasKey( "cpuTimeMillis" ) );
            Object cpuTime1 = data.get( "cpuTimeMillis" );
            assertThat( cpuTime1, instanceOf( Long.class ) );
            assertThat( data, hasKey( "resourceInformation" ) );
            Object ri = data.get( "resourceInformation" );
            assertThat( ri, instanceOf( Map.class ) );
            @SuppressWarnings( "unchecked" )
            Map<String,Object> resourceInformation = (Map<String,Object>) ri;
            assertEquals( "waiting", data.get( "status" ) );
            assertEquals( "EXCLUSIVE", resourceInformation.get( "lockMode" ) );
            assertEquals( "NODE", resourceInformation.get( "resourceType" ) );
            assertArrayEquals( new long[] {test.resource().getId()}, (long[]) resourceInformation.get( "resourceIds" ) );
            assertThat( data, hasKey( "waitTimeMillis" ) );
            Object waitTime1 = data.get( "waitTimeMillis" );
            assertThat( waitTime1, instanceOf( Long.class ) );

            // when
            data = getQueryListing( query );

            // then
            Long cpuTime2 = (Long) data.get( "cpuTimeMillis" );
            assertThat( cpuTime2, greaterThanOrEqualTo( (Long) cpuTime1 ) );
            Long waitTime2 = (Long) data.get( "waitTimeMillis" );
            assertThat( waitTime2, greaterThanOrEqualTo( (Long) waitTime1 ) );

            // ListPlannerAndRuntimeUsed
            // then
            assertThat( data, hasKey( "planner" ) );
            assertThat( data, hasKey( "runtime" ) );
            assertThat( data.get( "planner" ), instanceOf( String.class ) );
            assertThat( data.get( "runtime" ), instanceOf( String.class ) );

            // SpecificConnectionDetails

            // then
            assertThat( data, hasKey( "protocol" ) );
            assertThat( data, hasKey( "clientAddress" ) );
            assertThat( data, hasKey( "requestUri" ) );

            //ContainPageHitsAndPageFaults
            // then
            assertThat( data, hasEntry( equalTo( "pageHits" ), instanceOf( Long.class ) ) );
            assertThat( data, hasEntry( equalTo( "pageFaults" ), instanceOf( Long.class ) ) );
        }
    }

    @Test
    public void shouldProvideAllocatedBytes() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        final Node node;
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            node = test.resource();
            // when
            Map<String,Object> data = getQueryListing( query );

            // then
            assertThat( data, hasKey( "allocatedBytes" ) );
            Object allocatedBytes = data.get( "allocatedBytes" );
            assertThat( allocatedBytes, anyOf( nullValue(), (Matcher) allOf(
                    instanceOf( Long.class ), greaterThan( 0L ) ) ) );
        }

        try ( Resource<Node> test = test( () -> node, query ) )
        {
            // when
            Map<String,Object> data = getQueryListing( query );

            assertThat( data, hasKey( "allocatedBytes" ) );
            Object allocatedBytes = data.get( "allocatedBytes" );
            assertThat( allocatedBytes,
                    anyOf( nullValue(), (Matcher) allOf( instanceOf( Long.class ), greaterThan( 0L ) ) ) );
            assertSame( node, test.resource() );
        }
    }

    @Test
    public void shouldListActiveLocks() throws Exception
    {
        // given
        String query = "MATCH (x:X) SET x.v = 5 WITH count(x) AS num MATCH (y:Y) SET y.c = num";
        Set<Long> locked = new HashSet<>();
        try ( Resource<Node> test = test( () ->
        {
            for ( int i = 0; i < 5; i++ )
            {
                locked.add( db.createNode( label( "X" ) ).getId() );
            }
            return db.createNode( label( "Y" ) );
        }, query ) )
        {
            // when
            try ( Result rows = db.execute( "CALL dbms.listQueries() "
                    + "YIELD query AS queryText, queryId, activeLockCount "
                    + "WHERE queryText = $queryText "
                    + "CALL dbms.listActiveLocks(queryId) YIELD mode, resourceType, resourceId "
                    + "RETURN *", singletonMap( "queryText", query ) ) )
            {
                // then
                Set<Long> ids = new HashSet<>();
                Long lockCount = null;
                long rowCount = 0;
                while ( rows.hasNext() )
                {
                    Map<String,Object> row = rows.next();
                    Object resourceType = row.get( "resourceType" );
                    Object activeLockCount = row.get( "activeLockCount" );
                    if ( lockCount == null )
                    {
                        assertThat( "activeLockCount", activeLockCount, instanceOf( Long.class ) );
                        lockCount = (Long) activeLockCount;
                    }
                    else
                    {
                        assertEquals( "activeLockCount", lockCount, activeLockCount );
                    }
                    if ( ResourceTypes.LABEL.name().equals( resourceType ) )
                    {
                        assertEquals( "SHARED", row.get( "mode" ) );
                        assertEquals( 0L, row.get( "resourceId" ) );
                    }
                    else
                    {
                        assertEquals( "NODE", resourceType );
                        assertEquals( "EXCLUSIVE", row.get( "mode" ) );
                        ids.add( (Long) row.get( "resourceId" ) );
                    }
                    rowCount++;
                }
                assertEquals( locked, ids );
                assertNotNull( "activeLockCount", lockCount );
                assertEquals( lockCount.intValue(), rowCount ); // note: only true because query is blocked
            }
        }
    }

    @Test
    public void shouldOnlyGetActiveLockCountFromCurrentQuery() throws Exception
    {
        // given
        String query1 = "MATCH (x:X) SET x.v = 1";
        String query2 = "MATCH (y:Y) SET y.v = 2 WITH count(y) AS y MATCH (z:Z) SET z.v = y";
        try ( Resource<Node> test = test( () ->
        {
            for ( int i = 0; i < 5; i++ )
            {
                db.createNode( label( "X" ) );
            }
            db.createNode( label( "Y" ) );
            return db.createNode( label( "Z" ) );
        }, query1, query2 ) )
        {
            // when
            try ( Result rows = db.execute( "CALL dbms.listQueries() "
                    + "YIELD query AS queryText, queryId, activeLockCount "
                    + "WHERE queryText = $queryText "
                    + "CALL dbms.listActiveLocks(queryId) YIELD resourceId "
                    + "WITH queryText, queryId, activeLockCount, count(resourceId) AS allLocks "
                    + "RETURN *", singletonMap( "queryText", query2 ) ) )
            {
                assertTrue( "should have at least one row", rows.hasNext() );
                Map<String,Object> row = rows.next();
                Object activeLockCount = row.get( "activeLockCount" );
                Object allLocks = row.get( "allLocks" );
                assertFalse( "should have at most one row", rows.hasNext() );
                assertThat( "activeLockCount", activeLockCount, instanceOf( Long.class ) );
                assertThat( "allLocks", allLocks, instanceOf( Long.class ) );
                assertThat( (Long) activeLockCount, lessThan( (Long) allLocks ) );
            }
        }
    }

    @Test
    public void shouldContainSpecificConnectionDetails()
    {
        // when
        Map<String,Object> data = getQueryListing( "CALL dbms.listQueries" );

        // then
        assertThat( data, hasKey( "protocol" ) );
        assertThat( data, hasKey( "clientAddress" ) );
        assertThat( data, hasKey( "requestUri" ) );
    }

    @Test
    public void shouldContainPageHitsAndPageFaults() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            // when
            Map<String,Object> data = getQueryListing( query );

            // then
            assertThat( data, hasEntry( equalTo( "pageHits" ), instanceOf( Long.class ) ) );
            assertThat( data, hasEntry( equalTo( "pageFaults" ), instanceOf( Long.class ) ) );
        }
    }

    @Test
    public void shouldListUsedIndexes() throws Exception
    {
        // given
        String label = "IndexedLabel";
        String property = "indexedProperty";
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( label ) ).on( property ).create();
            tx.success();
        }
        ensureIndexesAreOnline();
        shouldListUsedIndexes( label, property );
    }

    private void ensureIndexesAreOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( SECONDS_TIMEOUT, SECONDS );
            tx.success();
        }
    }

    @Test
    public void shouldListUsedUniqueIndexes() throws Exception
    {
        // given
        String label = "UniqueLabel";
        String property = "uniqueProperty";
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label( label ) ).assertPropertyIsUnique( property ).create();
            tx.success();
        }
        ensureIndexesAreOnline();
        shouldListUsedIndexes( label, property );
    }

    @Test
    public void shouldListIndexesUsedForScans() throws Exception
    {
        // given
        final String QUERY = "MATCH (n:Node) USING INDEX n:Node(value) WHERE 1 < n.value < 10 SET n.value = 2";
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( "Node" ) ).on( "value" ).create();
            tx.success();
        }
        ensureIndexesAreOnline();
        try ( Resource<Node> test = test( () ->
        {
            Node node = db.createNode( label( "Node" ) );
            node.setProperty( "value", 5L );
            return node;
        }, QUERY ) )
        {
            // when
            Map<String,Object> data = getQueryListing( QUERY );

            // then
            assertThat( data, hasEntry( equalTo( "indexes" ), instanceOf( List.class ) ) );
            @SuppressWarnings( "unchecked" )
            List<Map<String,Object>> indexes = (List<Map<String,Object>>) data.get( "indexes" );
            assertEquals( "number of indexes used", 1, indexes.size() );
            Map<String,Object> index = indexes.get( 0 );
            assertThat( index, hasEntry( "identifier", "n" ) );
            assertThat( index, hasEntry( "label", "Node" ) );
            assertThat( index, hasEntry( "propertyKey", "value" ) );
        }
    }

    @Test
    public void shouldDisableCpuTimeTracking() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        db.setConfig( track_query_cpu_time, FALSE );
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }

        // then
        assertThat( data, hasEntry( equalTo( "cpuTimeMillis" ), nullValue() ) );
    }

    @Test
    public void cpuTimeTrackingShouldBeADynamicSetting() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data, hasEntry( equalTo( "cpuTimeMillis" ), notNullValue() ) );

        // when
        db.execute( "call dbms.setConfigValue('" + track_query_cpu_time.name() + "', 'false')" );
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data, hasEntry( equalTo( "cpuTimeMillis" ), nullValue() ) );

        // when
        db.execute( "call dbms.setConfigValue('" + track_query_cpu_time.name() + "', 'true')" );
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data, hasEntry( equalTo( "cpuTimeMillis" ), notNullValue() ) );
    }

    @Test
    public void shouldDisableHeapAllocationTracking() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        db.setConfig( track_query_allocation, FALSE );
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }

        // then
        assertThat( data, hasEntry( equalTo( "allocatedBytes" ), nullValue() ) );
    }

    @Test
    public void heapAllocationTrackingShouldBeADynamicSetting() throws Exception
    {
        // given
        String query = "MATCH (n) SET n.v = n.v + 1";
        Map<String,Object> data;

        // when
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data, hasEntry( equalTo( "allocatedBytes" ), anyOf( nullValue(), (Matcher) allOf(
                instanceOf( Long.class ), greaterThan( 0L ) ) ) ) );

        // when
        db.execute( "call dbms.setConfigValue('" + track_query_allocation.name() + "', 'false')" );
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data, hasEntry( equalTo( "allocatedBytes" ), nullValue() ) );

        // when
        db.execute( "call dbms.setConfigValue('" + track_query_allocation.name() + "', 'true')" );
        try ( Resource<Node> test = test( db::createNode, query ) )
        {
            data = getQueryListing( query );
        }
        // then
        assertThat( data, hasEntry( equalTo( "allocatedBytes" ), anyOf( nullValue(), (Matcher) allOf(
                instanceOf( Long.class ), greaterThan( 0L ) ) ) ) );
    }

    private void shouldListUsedIndexes( String label, String property ) throws Exception
    {
        // given
        final String QUERY1 = "MATCH (n:" + label + "{" + property + ":5}) USING INDEX n:" + label + "(" + property +
                ") SET n." + property + " = 3";
        try ( Resource<Node> test = test( () ->
        {
            Node node = db.createNode( label( label ) );
            node.setProperty( property, 5L );
            return node;
        }, QUERY1 ) )
        {
            // when
            Map<String,Object> data = getQueryListing( QUERY1 );

            // then
            assertThat( data, hasEntry( equalTo( "indexes" ), instanceOf( List.class ) ) );
            @SuppressWarnings( "unchecked" )
            List<Map<String,Object>> indexes = (List<Map<String,Object>>) data.get( "indexes" );
            assertEquals( "number of indexes used", 1, indexes.size() );
            Map<String,Object> index = indexes.get( 0 );
            assertThat( index, hasEntry( "identifier", "n" ) );
            assertThat( index, hasEntry( "label", label ) );
            assertThat( index, hasEntry( "propertyKey", property ) );
        }

        // given
        final String QUERY2 = "MATCH (n:" + label + "{" + property + ":3}) USING INDEX n:" + label + "(" + property +
                ") MATCH (u:" + label + "{" + property + ":4}) USING INDEX u:" + label + "(" + property +
                ") CREATE (n)-[:KNOWS]->(u)";
        try ( Resource<Node> test = test( () ->
        {
            Node node = db.createNode( label( label ) );
            node.setProperty( property, 4L );
            return node;
        }, QUERY2 ) )
        {
            // when
            Map<String,Object> data = getQueryListing( QUERY2 );

            // then
            assertThat( data, hasEntry( equalTo( "indexes" ), instanceOf( List.class ) ) );
            @SuppressWarnings( "unchecked" )
            List<Map<String,Object>> indexes = (List<Map<String,Object>>) data.get( "indexes" );
            assertEquals( "number of indexes used", 2, indexes.size() );

            Map<String,Object> index1 = indexes.get( 0 );
            assertThat( index1, hasEntry( "identifier", "n" ) );
            assertThat( index1, hasEntry( "label", label ) );
            assertThat( index1, hasEntry( "propertyKey", property ) );

            Map<String,Object> index2 = indexes.get( 1 );
            assertThat( index2, hasEntry( "identifier", "u" ) );
            assertThat( index2, hasEntry( "label", label ) );
            assertThat( index2, hasEntry( "propertyKey", property ) );
        }
    }

    private Map<String,Object> getQueryListing( String query )
    {
        try ( Result rows = db.execute( "CALL dbms.listQueries" ) )
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

    private <T extends PropertyContainer> Resource<T> test( Supplier<T> setup, String... queries )
            throws InterruptedException, ExecutionException
    {
        CountDownLatch resourceLocked = new CountDownLatch( 1 );
        CountDownLatch listQueriesLatch = new CountDownLatch( 1 );
        CountDownLatch finishQueriesLatch = new CountDownLatch( 1 );
        T resource;
        try ( Transaction tx = db.beginTx() )
        {
            resource = setup.get();
            tx.success();
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
                    db.execute( query ).close();
                }
                tx.success();
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                throw new RuntimeException( t );
            }
            finally
            {
                finishQueriesLatch.countDown();
            }
            return null;
        }, null, waitingWhileIn( GraphDatabaseFacade.class, "execute" ), SECONDS_TIMEOUT, SECONDS );

        return new Resource<>( listQueriesLatch, finishQueriesLatch, resource );
    }
}
