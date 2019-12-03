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
package org.neo4j.cypher.result;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Optional;

import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.values.storable.Values.stringValue;

class NaiveQuerySubscriptionTest
{
    @Test
    void shouldHandleAskingForAllResultsImmediately() throws Exception
    {
        // Given
        QuerySubscriber subscriber = mock( QuerySubscriber.class );
        InOrder inOrder = inOrder( subscriber );
        Tester tester = new Tester( subscriber, 3 );

        // When
        tester.request( Long.MAX_VALUE );

        // Then
        assertFalse( tester.await() );

        inOrder.verify( subscriber ).onResult( 2 );

        //record 1
        inOrder.verify( subscriber ).onRecord();
        inOrder.verify( subscriber ).onField( 0, stringValue( "hello" ) );
        inOrder.verify( subscriber ).onField( 1, stringValue( "there" ) );
        inOrder.verify( subscriber ).onRecordCompleted();
        //record 2
        inOrder.verify( subscriber ).onRecord();
        inOrder.verify( subscriber ).onField( 0, stringValue( "hello" ) );
        inOrder.verify( subscriber ).onField( 1, stringValue( "there" ) );
        inOrder.verify( subscriber ).onRecordCompleted();
        //record 3
        inOrder.verify( subscriber ).onRecord();
        inOrder.verify( subscriber ).onField( 0, stringValue( "hello" ) );
        inOrder.verify( subscriber ).onField( 1, stringValue( "there" ) );
        inOrder.verify( subscriber ).onRecordCompleted();

        inOrder.verify( subscriber ).onResultCompleted( QueryStatistics.empty() );
        inOrder.verifyNoMoreInteractions( );
    }

    @Test
    void shouldHandleAskingForResultsReactively() throws Exception
    {
        // Given
        QuerySubscriber subscriber = mock( QuerySubscriber.class );
        InOrder inOrder = inOrder( subscriber );
        Tester tester = new Tester( subscriber, 3 );

        //When, asking for the first record
        tester.request( 1 );
        assertTrue( tester.await() );

        //Then
        inOrder.verify( subscriber ).onResult( 2 );

        //record 1
        inOrder.verify( subscriber ).onRecord();
        inOrder.verify( subscriber ).onField( 0, stringValue( "hello" ) );
        inOrder.verify( subscriber ).onField( 1, stringValue( "there" ) );
        inOrder.verify( subscriber ).onRecordCompleted();

        //When, asking for the second record
        tester.request( 1 );

        //Then
        assertTrue( tester.await() );

        //record 2
        inOrder.verify( subscriber ).onRecord();
        inOrder.verify( subscriber ).onField( 0, stringValue( "hello" ) );
        inOrder.verify( subscriber ).onField( 1, stringValue( "there" ) );
        inOrder.verify( subscriber ).onRecordCompleted();

        //When, asking for the third and last record
        tester.request( 1 );

        //Then
        assertFalse( tester.await() );

        //record 3
        inOrder.verify( subscriber ).onRecord();
        inOrder.verify( subscriber ).onField( 0, stringValue( "hello" ) );
        inOrder.verify( subscriber ).onField( 1, stringValue( "there" ) );
        inOrder.verify( subscriber ).onRecordCompleted();

        inOrder.verify( subscriber ).onResultCompleted( QueryStatistics.empty() );
        inOrder.verifyNoMoreInteractions( );
    }

    private class Tester extends NaiveQuerySubscription
    {
        private final int numberOfRecords;

        Tester( QuerySubscriber subscriber, int numberOfRecords )
        {
            super( subscriber );
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public String[] fieldNames()
        {
            return new String[]{"a", "b"};
        }

        @Override
        public ConsumptionState consumptionState()
        {
            return null;
        }

        @Override
        public QueryStatistics queryStatistics()
        {
            return QueryStatistics.empty();
        }

        @Override
        public QueryProfile queryProfile()
        {
            return null;
        }

        @Override
        public void close()
        {

        }

        @Override
        public <E extends Exception> void accept( QueryResult.QueryResultVisitor<E> visitor ) throws E
        {
            for ( int i = 0; i < numberOfRecords; i++ )
            {
                visitor.visit( record( stringValue( "hello" ), stringValue( "there" ) ) );
            }
        }

        @Override
        public Optional<Long> totalAllocatedMemory()
        {
            return Optional.empty();
        }
    }

    ResultRecord record( AnyValue...fields )
    {
        return new ResultRecord(  fields );
    }

    private class ResultRecord implements QueryResult.Record
    {
        private final AnyValue[] fields;

        private ResultRecord( AnyValue[] fields )
        {
            this.fields = fields;
        }

        @Override
        public AnyValue[] fields()
        {
            return fields;
        }
    }
}
