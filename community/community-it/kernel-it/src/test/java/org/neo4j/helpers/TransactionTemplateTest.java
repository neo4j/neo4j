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
package org.neo4j.helpers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DbmsExtension
class TransactionTemplateTest
{
    @Inject
    private GraphDatabaseService db;

    private TransactionTemplate template;
    private CountingMonitor monitor;

    @BeforeEach
    void setUp()
    {
        monitor = new CountingMonitor();
        template = new TransactionTemplate()
                .with( db )
                .monitor( monitor )
                .retries( 5 )
                .backoff( 3, TimeUnit.MILLISECONDS );
    }

    @Test
    void shouldForceUserToCallWith()
    {
        TransactionTemplate transactionTemplate = new TransactionTemplate();
        Throwable cause = assertThrows( RuntimeException.class, () -> transactionTemplate.execute( transaction -> null ) ).getCause();
        assertThat( cause, instanceOf( IllegalArgumentException.class ) );
        assertEquals( "You need to call 'with(GraphDatabaseService)' on the template in order to use it", cause.getMessage() );
    }

    @Test
    void validateGraphDatabaseService()
    {
        assertThrows( NullPointerException.class, () -> template.with( null ) );
    }

    @Test
    void validateRetires()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> template.retries( -1 ) );
        assertEquals( "Number of retries must be greater than or equal to 0", exception.getMessage() );
    }

    @Test
    void validateBackoff()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> template.backoff( -10, TimeUnit.SECONDS ) );
        assertEquals( "Backoff time must be a positive number", exception.getMessage() );
    }

    @Test
    void validateMonitor()
    {
        assertThrows( NullPointerException.class, () -> template.monitor( null ) );
    }

    @Test
    void validateRetryOn()
    {
        assertThrows( NullPointerException.class, () -> template.retryOn( null ) );
    }

    @Test
    void shouldRetryOnError()
    {
        IllegalArgumentException ex = new IllegalArgumentException();
        template.execute( new FailingRetryConsumer( 3, ex ) );

        assertThat( monitor.numRetry, is( 3 ) );
        assertThat( monitor.failures, contains( ex, ex, ex ) );
        assertThat( monitor.fails, empty() );
    }

    @Test
    void shouldFailIfAllRetiresFail()
    {
        IllegalArgumentException ex = new IllegalArgumentException();
        try
        {
            template.execute( new FailingRetryConsumer( 10, ex ) );
        }
        catch ( TransactionFailureException ignored )
        {
        }

        assertThat( monitor.numRetry, is( 5 ) );
        assertThat( monitor.failures, contains( ex, ex, ex, ex, ex, ex ) ); // 5 retires results in 6 total failures
        assertThat( monitor.fails, contains( ex ) );
    }

    @Test
    void defaultExceptionsForExit()
    {
        Error error = new Error();
        TransactionTerminatedException terminatedException = new TransactionTerminatedException( Status.Transaction.Terminated );

        try
        {
            template.execute( (Consumer<Transaction>) tx ->
            {
                throw error;
            } );
        }
        catch ( TransactionFailureException ex )
        {
            // Expected
        }

        try
        {
            template.execute( (Consumer<Transaction>) tx ->
            {
                throw terminatedException;
            } );
        }
        catch ( TransactionFailureException ignored )
        {
        }

        assertThat( monitor.numRetry, is( 0 ) );
        assertThat( monitor.failures, contains( error, terminatedException ) );
        assertThat( monitor.fails, contains( error, terminatedException ) );
    }

    @Test
    void overrideRetryExceptions()
    {
        template = template.retryOn( e -> !(e instanceof IllegalArgumentException) );
        IllegalArgumentException e = new IllegalArgumentException();
        try
        {
            template.execute( (Consumer<Transaction>) tx ->
            {
                throw e;
            } );
        }
        catch ( TransactionFailureException ignored )
        {
        }

        assertThat( monitor.numRetry, is( 0 ) );
        assertThat( monitor.failures, contains( e ) );
        assertThat( monitor.fails, contains( e ) );
    }

    @Test
    void overrideRetryShouldOverrideDefaults()
    {
        template = template.retryOn( e -> !(e instanceof IllegalArgumentException) );

        TransactionTerminatedException fakeException = new TransactionTerminatedException( Status.Transaction.Terminated );
        template.execute( new FailingRetryConsumer( 1, fakeException ) );

        assertThat( monitor.numRetry, is( 1 ) );
        assertThat( monitor.failures, contains( fakeException ) );
        assertThat( monitor.fails, empty() );
    }

    private static class FailingRetryConsumer implements Consumer<Transaction>
    {
        private final int successAfter;
        private final RuntimeException fakeException;
        private int tries;

        private FailingRetryConsumer( int successAfter, RuntimeException fakeException )
        {
            this.successAfter = successAfter;
            this.fakeException = fakeException;
        }

        @Override
        public void accept( Transaction transaction )
        {
            if ( tries++ < successAfter )
            {
                throw fakeException;
            }
        }
    }

    private static class CountingMonitor implements TransactionTemplate.Monitor
    {
        int numRetry;
        List<Throwable> fails = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();

        @Override
        public void failure( Throwable ex )
        {
            failures.add( ex );
        }

        @Override
        public void failed( Throwable ex )
        {
            fails.add( ex );
        }

        @Override
        public void retrying()
        {
            numRetry++;
        }
    }
}
