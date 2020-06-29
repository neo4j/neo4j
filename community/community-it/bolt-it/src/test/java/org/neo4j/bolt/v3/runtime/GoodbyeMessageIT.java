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
package org.neo4j.bolt.v3.runtime;

import org.assertj.core.api.Condition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceives;
import static org.neo4j.bolt.testing.TransportTestUtil.serverImmediatelyDisconnects;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;

public class GoodbyeMessageIT extends BoltV3TransportBase
{
    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldCloseConnectionInConnected( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        connection.connect( address )
                .send( util.acceptedVersions( 3, 2, 1, 0 ) );
        assertThat( connection ).satisfies( eventuallyReceives( new byte[]{0, 0, 0, 3} ) );

        // When
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection ).satisfies( serverImmediatelyDisconnects() );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldCloseConnectionInReady( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection ).satisfies( serverImmediatelyDisconnects() );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldCloseConnectionInStreaming( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) ) );
        assertThat( connection ).satisfies(
                util.eventuallyReceives( msgSuccess( message -> assertThat(message)
                        .containsEntry( "fields",  asList( "a", "a_squared" )  )
                        .containsKey("t_first" ) ) ) );
        // you shall be in the streaming state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection ).satisfies( serverImmediatelyDisconnects() );
        assertThat( server ).satisfies( eventuallyClosesTransaction() );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldCloseConnectionInFailed( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new RunMessage( "I am sending you to failed state!" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgFailure( Status.Statement.SyntaxError,
                        String.format( "line 1, column 1" ) ) ) );
        // you shall be in the failed state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection ).satisfies( serverImmediatelyDisconnects() );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldCloseConnectionInTxReady( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        // you shall be in tx_ready state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );

        // Then
        assertThat( connection ).satisfies( serverImmediatelyDisconnects() );
        assertThat( server ).satisfies( eventuallyClosesTransaction() );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldCloseConnectionInTxStreaming( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( new BeginMessage(), new RunMessage( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess(),
                msgSuccess( message -> assertThat( message ).containsKey( "t_first" ).containsEntry( "fields", asList( "a", "a_squared" ) ) ) ) );

        // you shall be in the tx_streaming state now
        connection.send( util.chunk( GOODBYE_MESSAGE ) );
        // Then
        assertThat( connection ).satisfies( serverImmediatelyDisconnects() );
        assertThat( server ).satisfies( eventuallyClosesTransaction() );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "argumentsProvider" )
    public void shouldDropConnectionImmediatelyAfterGoodbye( Class<? extends TransportConnection> connectionClass ) throws Exception
    {
        init( connectionClass );

        // Given
        negotiateBoltV3();

        // When
        connection.send( util.chunk( GOODBYE_MESSAGE, ResetMessage.INSTANCE, new RunMessage( "RETURN 1" ) ) );

        // Then
        assertThat( connection ).satisfies( serverImmediatelyDisconnects() );
    }

    private static Condition<Neo4jWithSocket> eventuallyClosesTransaction()
    {
        return new Condition<>( server ->
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
        }, "Eventually close all transactions" );
    }

    private static Set<KernelTransactionHandle> getActiveTransactions( Neo4jWithSocket server )
    {
        var gdb = (GraphDatabaseAPI) server.graphDatabaseService();
        return gdb.getDependencyResolver().resolveDependency( KernelTransactions.class ).activeTransactions();
    }
}
