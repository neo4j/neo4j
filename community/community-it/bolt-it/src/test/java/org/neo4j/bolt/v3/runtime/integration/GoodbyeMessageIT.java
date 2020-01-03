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
package org.neo4j.bolt.v3.runtime.integration;

import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.serverImmediatelyDisconnects;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;

public class GoodbyeMessageIT extends BoltV3TransportBase
{
    @Test
    public void shouldCloseConnectionInConnected() throws Throwable
    {
        // Given
        connection.connect( address )
                .send( util.acceptedVersions( 3, 2, 1, 0 ) );
        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 3} ) );

        // When
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection, serverImmediatelyDisconnects() );
    }

    @Test
    public void shouldCloseConnectionInReady() throws Throwable
    {
        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection, serverImmediatelyDisconnects() );
    }

    @Test
    public void shouldCloseConnectionInStreaming() throws Throwable
    {
        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) ) );
        Matcher<Map<? extends String,?>> entryFieldMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess( allOf( entryFieldMatcher, hasKey( "t_first" ) ) ) ) );
        // you shall be in the streaming state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection, serverImmediatelyDisconnects() );
        assertThat( server, eventuallyClosesTransaction() );
    }

    @Test
    public void shouldCloseConnectionInFailed() throws Throwable
    {
        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new RunMessage( "I am sending you to failed state!" ) ) );
        assertThat( connection, util.eventuallyReceives(
                msgFailure( Status.Statement.SyntaxError,
                        String.format( "Invalid input 'I': expected <init> (line 1, column 1 (offset: 0))%n" +
                                "\"I am sending you to failed state!\"%n" +
                                " ^" ) ) ) );
        // you shall be in the failed state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection, serverImmediatelyDisconnects() );
    }

    @Test
    public void shouldCloseConnectionInTxReady() throws Throwable
    {
        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
        // you shall be in tx_ready state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection, serverImmediatelyDisconnects() );
        assertThat( server, eventuallyClosesTransaction() );
    }

    @Test
    public void shouldCloseConnectionInTxStreaming() throws Throwable
    {
        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new BeginMessage(), new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) ) );
        Matcher<Map<? extends String,?>> entryFieldMatcher = hasEntry( is( "fields" ), equalTo( asList( "a", "a_squared" ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess(), msgSuccess( allOf( entryFieldMatcher, hasKey( "t_first" ) ) ) ) );

        // you shall be in the tx_streaming state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );
        // Then
        assertThat( connection, serverImmediatelyDisconnects() );
        assertThat( server, eventuallyClosesTransaction() );
    }

    @Test
    public void shouldDropConnectionImmediatelyAfterGoodbye() throws Throwable
    {
        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( GOODBYE_MESSAGE, ResetMessage.INSTANCE, new RunMessage( "RETURN 1" ) ) );

        // Then
        assertThat( connection, serverImmediatelyDisconnects() );
    }

    private static Matcher<Neo4jWithSocket> eventuallyClosesTransaction()
    {
        return new TypeSafeMatcher<Neo4jWithSocket>()
        {
            @Override
            public void describeTo( org.hamcrest.Description description )
            {
                description.appendText( "Eventually close all transactions" );
            }

            @Override
            protected boolean matchesSafely( Neo4jWithSocket server )
            {
                BooleanSupplier condition = () -> getActiveTransactions( server ).size() == 0;
                try
                {
                    Predicates.await( condition, 2, TimeUnit.SECONDS );
                    return true;
                }
                catch ( Exception e )
                {
                    return false;
                }
            }

            private Set<KernelTransactionHandle> getActiveTransactions( Neo4jWithSocket server )
            {
                GraphDatabaseAPI gdb = (GraphDatabaseAPI) server.graphDatabaseService();
                return gdb.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
            }
        };
    }
}
