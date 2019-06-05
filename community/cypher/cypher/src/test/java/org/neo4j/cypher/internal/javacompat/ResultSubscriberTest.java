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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryExecutionType.QueryType;
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
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

class ResultSubscriberTest
{
    @ParameterizedTest
    @EnumSource( value = QueryType.class, names = {"READ_ONLY", "READ_WRITE"} )
    void shouldGetAllResultsViaAcceptAnClose( QueryType type )
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a", "b" )
                .withType( type )
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

    @ParameterizedTest
    @EnumSource( value = QueryType.class, names = {"READ_ONLY", "READ_WRITE"} )
    void shouldGetPartialResultsViaAcceptAndClose( QueryType type )
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a", "b" )
                .withType( type )
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

    @ParameterizedTest
    @EnumSource( value = QueryType.class, names = {"READ_ONLY", "READ_WRITE"} )
    void shouldGetAllResultsViaIterator( QueryType type )
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        queryExecution( subscriber )
                .withType( type )
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

    @ParameterizedTest
    @EnumSource( value = QueryType.class, names = {"READ_ONLY", "READ_WRITE"} )
    void shouldCloseAfterExhaustingIterator( QueryType type )
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withType( type )
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

    @Test
    void shouldCloseWriteOnlyQueriesDirectly( )
    {
        // Given
        TestQueryExecution queryExecution = queryExecution( subscriber() )
                .withType( WRITE )
                .queryExecution();

        // Then
        assertTrue( queryExecution.isClosed() );
    }

    @Test
    void shouldCloseReadWriteQueriesWithoutColumnsDirectly( )
    {
        // Given
        TestQueryExecution queryExecution = queryExecution( subscriber() )
                .withType( READ_WRITE )
                .queryExecution();

        // Then
        assertTrue( queryExecution.isClosed() );
    }

    @Test
    void shouldNotMaterializeNorCloseReadOnlyResult()
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a" )
                .withRecords( record( "hello" ) )
                .withType( READ_ONLY )
                .queryExecution();

        // Then
        assertFalse( subscriber.isMaterialized() );
        assertFalse( queryExecution.isClosed() );
    }

    @Test
    void shouldMaterializeButNotCloseReadWriteResult()
    {
        // Given
        ResultSubscriber subscriber = subscriber();
        TestQueryExecution queryExecution = queryExecution( subscriber )
                .withFields( "a" )
                .withRecords( record( "hello" ) )
                .withType( READ_WRITE )
                .queryExecution();

        // Then
        assertTrue( subscriber.isMaterialized() );
        assertFalse( queryExecution.isClosed() );
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
        private QueryExecutionType type = QueryExecutionType.query( READ_ONLY );
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

        QueryExecutionBuilder withType( QueryType queryType )
        {
            this.type = QueryExecutionType.query( queryType );
            return this;
        }

        QueryExecutionBuilder withRecords( AnyValue[]... results )
        {
            this.results = asList( results ).iterator();
            return this;
        }

        TestQueryExecution queryExecution()
        {
            TestQueryExecution execution = new TestQueryExecution( subscriber, fields, type, results );
            subscriber.init( execution );
            return execution;
        }
    }

    private static class TestQueryExecution implements QueryExecution
    {
        private final QuerySubscriber subscriber;
        private final String[] fields;
        private final QueryExecutionType type;
        private final Iterator<AnyValue[]> results;
        private long demand;
        private long served;
        private boolean cancelled;

        private TestQueryExecution( QuerySubscriber subscriber, String[] fields,
                QueryExecutionType type, Iterator<AnyValue[]> results )
        {
            this.subscriber = subscriber;
            this.fields = fields;
            this.type = type;
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
            return type;
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
                for ( int offset = 0; offset < record.length; offset++ )
                {
                    subscriber.onField( offset, record[offset] );
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
