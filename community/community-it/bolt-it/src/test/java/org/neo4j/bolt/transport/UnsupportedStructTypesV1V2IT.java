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
package org.neo4j.bolt.transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackedOutputArray;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.PathValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.packstream.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.packstream.example.Nodes.ALICE;
import static org.neo4j.bolt.packstream.example.Paths.ALL_PATHS;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.kernel.api.exceptions.Status.Statement.TypeError;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class UnsupportedStructTypesV1V2IT extends AbstractBoltTransportsTest
{
    public static final byte DEFAULT_SIGNATURE = RunMessage.SIGNATURE;

    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
    }

    @AfterEach
    public void cleanup()
    {
        server.shutdownDatabase();
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailWhenNullKeyIsSent( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsgWithNullKey() ) );

        assertThat( connection ).satisfies( util.eventuallyReceives(
                msgFailure( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." ) ) );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailWhenDuplicateKey( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsgWithDuplicateKey() ) );

        assertThat( connection ).satisfies( util.eventuallyReceives( msgFailure( Status.Request.Invalid, "Duplicate map key `key1`." ) ) );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailWhenNodeIsSentWithRun(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        testFailureWithV1Value( ALICE, "Node" );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailWhenRelationshipIsSentWithRun(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        testFailureWithV1Value( ALICE_KNOWS_BOB, "Relationship" );
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldFailWhenPathIsSentWithRun( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        for ( PathValue path : ALL_PATHS )
        {
            try
            {
                testFailureWithV1Value( path, "Path" );
            }
            finally
            {
                reconnect();
            }
        }
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldTerminateConnectionWhenUnknownMessageIsSent(
            Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createUnknownMsg() ) );

        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    private void testFailureWithV1Value( AnyValue value, String description ) throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWithUnknownValue( value ) ) );

        assertThat( connection ).satisfies(
                util.eventuallyReceives( msgFailure( TypeError, description + " values cannot be unpacked with this version of bolt." ) ) );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
    }

    private byte[] createRunWithUnknownValue( AnyValue value ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 2, RunMessage.SIGNATURE );
        packer.pack( "RETURN $x" );
        packer.packMapHeader( 1 );
        packer.pack( "x" );
        packer.pack( value );

        return out.bytes();
    }

    private byte[] createUnknownMsg() throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 0, DEFAULT_SIGNATURE );

        return out.bytes();
    }

    private byte[] createMsgWithNullKey() throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 2, DEFAULT_SIGNATURE );
        packer.pack( "Text" );
        packMapWithNullKey( packer );

        return out.bytes();
    }

    private byte[] createMsgWithDuplicateKey() throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 2, DEFAULT_SIGNATURE );
        packer.pack( "Text" );
        packMapWithDuplicateKey( packer );

        return out.bytes();
    }

    private static void packMapWithNullKey( Neo4jPack.Packer packer ) throws IOException
    {
        packer.packMapHeader( 2 );
        packer.pack( "key1" );
        packer.pack( ValueUtils.of( null ) );
        packer.pack( ValueUtils.of( null ) );
        packer.pack( "value1" );
    }

    private static void packMapWithDuplicateKey( Neo4jPack.Packer packer ) throws IOException
    {
        packer.packMapHeader( 2 );
        packer.pack( "key1" );
        packer.pack( "value1" );
        packer.pack( "key1" );
        packer.pack( "value2" );
    }
}
