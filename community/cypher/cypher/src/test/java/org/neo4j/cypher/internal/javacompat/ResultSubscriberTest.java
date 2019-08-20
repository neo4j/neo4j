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
package org.neo4j.cypher.internal.javacompat;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

class ResultSubscriberTest
{
   @Test
    void shouldGetAllResultsViaAcceptAndClose( )
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a", "b" )
                .withRecords(
                        record( 11, 12 ),
                        record( 21, 22 ),
                        record( 31, 32 ) )
                .queryExecution();

        // When
        List<Map<String,Object>> records = new ArrayList<>();
        subscriber.accept( row -> {
            records.add( map( "a", row.get( "a" ), "b", row.get( "b" ) ) );
            return true;
        } );

        // Then
        assertThat( records, equalTo( asList(
                map( "a", 11, "b", 12 ),
                map( "a", 21, "b", 22 ),
                map( "a", 31, "b", 32 )
        ) ) );
        assertTrue( queryExecution.isClosed() );
    }

    @Test
    void hasNextShouldNotChangeResultAsString()
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a", "b" )
                .withRecords(
                        record( 11, 12 ),
                        record( 21, 22 ),
                        record( 31, 32 ) )
                .queryExecution();

        // When
        //noinspection ResultOfMethodCallIgnored
        subscriber.hasNext();

        // Then
        assertThat( subscriber.resultAsString(),
                    equalTo( String.format( "+---------+%s| a  | b  |%s+---------+%s| 11 | 12 |%s| 21 | 22 |%s| 31 | 32 |%s+---------+%s3 rows%s",
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator()
                    ) ) );
        assertTrue( queryExecution.isClosed() );
    }

    @Test
    void nextShouldChangeResultAsString()
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a", "b" )
                .withRecords(
                        record( 11, 12 ),
                        record( 21, 22 ),
                        record( 31, 32 ) )
                .queryExecution();

        // When
        Map<String,Object> first = subscriber.next();

        // Then
        assertThat( first, equalTo( Map.of(
                "a", 11,
                "b", 12
        ) ) );
        assertThat( subscriber.resultAsString(),
                    equalTo( String.format( "+---------+%s| a  | b  |%s+---------+%s| 21 | 22 |%s| 31 | 32 |%s+---------+%s2 rows%s",
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator(),
                                            System.lineSeparator()
                    ) ) );
        assertTrue( queryExecution.isClosed() );
    }

    @Test
    void shouldGetPartialResultsViaAcceptAndClose( )
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a", "b" )
                .withRecords(
                        record( 11, 12 ),
                        record( 21, 22 ),
                        record( 31, 32 ) )
                .queryExecution();

        // When
        List<Map<String,Object>> records = new ArrayList<>();
        subscriber.accept( row -> {
            records.add( map( "a", row.get( "a" ), "b", row.get( "b" ) ) );
            return false;
        } );

        // Then
        assertThat( records, equalTo( singletonList( map( "a", 11, "b", 12 ) ) ) );
        assertTrue( queryExecution.isClosed() );
    }

    @Test
    void shouldGetAllResultsViaIterator( )
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        queryExecution( subscriber )
                .withFields( "a", "b" )
                .withRecords(
                        record( 11, 12 ),
                        record( 21, 22 ),
                        record( 31, 32 ) )
                .queryExecution();

        // When
        List<Map<String,Object>> records = Iterators.asList( subscriber );

        // Then
        assertThat( records, equalTo( asList(
                map( "a", 11, "b", 12 ),
                map( "a", 21, "b", 22 ),
                map( "a", 31, "b", 32 )
        ) ) );
    }

    @Test
    void shouldCloseAfterExhaustingIterator()
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a", "b" )
                .withRecords(
                        record( 11, 12 ),
                        record( 21, 22 ),
                        record( 31, 32 ) )
                .queryExecution();

        // When
        assertTrue( subscriber.hasNext() );
        assertEquals( subscriber.next(), map( "a", 11, "b", 12 ) );
        assertFalse( queryExecution.isClosed() );

        assertTrue( subscriber.hasNext() );
        assertEquals( subscriber.next(), map( "a", 21, "b", 22 ) );
        assertFalse( queryExecution.isClosed() );

        assertTrue( subscriber.hasNext() );
        assertEquals( subscriber.next(), map( "a", 31, "b", 32 ) );
        assertFalse( subscriber.hasNext() );

        assertTrue( queryExecution.isClosed() );
    }

    private static ResultSubscriber subscriber()
    {
        return new ResultSubscriber( mock( TransactionalContext.class, RETURNS_DEEP_STUBS ) );
    }

    private static AnyValue[] record( Object... values )
    {
        AnyValue[] record = new AnyValue[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            record[i] = ValueUtils.of( values[i] );
        }

        return record;
    }

    private static QueryExecutionBuilder queryExecution( ResultSubscriber resultSubscriber )
    {
        return new QueryExecutionBuilder( resultSubscriber );
    }

    private static class QueryExecutionBuilder
    {
        private final ResultSubscriber subscriber;
        private String[] fields = new String[0];
        private Iterator<AnyValue[]> results = Collections.emptyIterator();

        private QueryExecutionBuilder( ResultSubscriber subscriber )
        {
            this.subscriber = subscriber;
        }

        QueryExecutionBuilder withFields( String... fields )
        {
            this.fields = fields;
            return this;
        }

        QueryExecutionBuilder withRecords( AnyValue[]... results )
        {
            this.results = asList( results ).iterator();
            return this;
        }

        TestQueryExecution queryExecution()
        {
            TestQueryExecution execution = new TestQueryExecution( subscriber, fields, results );
            subscriber.init( execution );
            return execution;
        }
    }

    private static class TestQueryExecution implements QueryExecution
    {
        private final QuerySubscriber subscriber;
        private final String[] fields;
        private final Iterator<AnyValue[]> results;
        private long demand;
        private long served;
        private boolean cancelled;

        private TestQueryExecution( QuerySubscriber subscriber, String[] fields,
                Iterator<AnyValue[]> results )
        {
            this.subscriber = subscriber;
            this.fields = fields;
            this.results = results;
            try
            {
                subscriber.onResult( fields.length );
            }
            catch ( Exception e )
            {
                throw new AssertionError( e );
            }
        }

        @Override
        public QueryExecutionType executionType()
        {
            return QueryExecutionType.query( READ_ONLY );
        }

        @Override
        public ExecutionPlanDescription executionPlanDescription()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Notification> getNotifications()
        {
            return Collections.emptyList();
        }

        @Override
        public String[] fieldNames()
        {
            return fields;
        }

        @Override
        public void request( long numberOfRecords ) throws Exception
        {
            demand += numberOfRecords;
            if ( demand < 0 )
            {
                demand = Long.MAX_VALUE;
            }

            while ( served < demand && results.hasNext() && !cancelled )
            {
                subscriber.onRecord();
                AnyValue[] record = results.next();
                for ( AnyValue anyValue : record )
                {
                    subscriber.onField( anyValue );
                }
                subscriber.onRecordCompleted();
                served++;
            }

            if ( served == demand )
            {
                subscriber.onResultCompleted( QueryStatistics.empty() );
            }
        }

        @Override
        public void cancel()
        {
            this.cancelled = true;
        }

        @Override
        public boolean await()
        {
            return results.hasNext();
        }

        public boolean isClosed()
        {
            return cancelled;
        }
    }
}
