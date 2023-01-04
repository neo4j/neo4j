/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import org.neo4j.bolt.packstream.Neo4jPack;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transaction.StatementProcessorTxManager;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.bolt.v3.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class TransactionManagerCleanupIT extends AbstractBoltTransportsTest
{

    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    void shouldIncreaseAndDecreaseTxCount( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name )
            throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );
        var txManager = getStatementProcessorTxManager( server );

        Assertions.assertThat(txManager.transactionCount()).isEqualTo(0);

        // When
        connection.connect( address )
                  .send( TransportTestUtil.defaultAcceptedVersions() )
                  .send( util.defaultAuth() )
                  .send( util.chunk( new BeginMessage() ) );

        Assertions.assertThat( connection ).satisfies( TransportTestUtil.eventuallyReceivesSelectedProtocolVersion() )
                  .satisfies( util.eventuallyReceives( msgSuccess() ) )
                  .satisfies( util.eventuallyReceives( msgSuccess() ) );

        Assertions.assertThat(txManager.transactionCount()).isEqualTo(1);

        connection.send( util.chunk( RollbackMessage.ROLLBACK_MESSAGE ) );
        Assertions.assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        Assertions.assertThat(txManager.transactionCount()).isEqualTo(0);

    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    void shouldDecrementProviderCountOnConnectionClosure( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name )
            throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );
        var txManager = getStatementProcessorTxManager( server );

        Assertions.assertThat(txManager.statementProcessorProviderCount()).isEqualTo(0);

        // When
        connection.connect( address )
                  .send( TransportTestUtil.defaultAcceptedVersions() )
                  .send( util.defaultAuth() );

        Assertions.assertThat( connection ).satisfies( TransportTestUtil.eventuallyReceivesSelectedProtocolVersion() )
                  .satisfies( util.eventuallyReceives( msgSuccess() ) );

        Assertions.assertThat(txManager.statementProcessorProviderCount()).isEqualTo(1);

        connection.send( util.chunk( GoodbyeMessage.GOODBYE_MESSAGE ) );
        Assertions.assertThat( connection ).satisfies( TransportTestUtil.eventuallyDisconnects() );

        Assertions.assertThat(txManager.statementProcessorProviderCount()).isEqualTo(0);
    }

    private static StatementProcessorTxManager getStatementProcessorTxManager( Neo4jWithSocket server )
    {
        return ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver().resolveDependency( StatementProcessorTxManager.class );
    }
}
