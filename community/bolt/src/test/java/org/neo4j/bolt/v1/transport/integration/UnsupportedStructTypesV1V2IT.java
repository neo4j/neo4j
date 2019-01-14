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
package org.neo4j.bolt.v1.transport.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.v1.messaging.BoltRequestMessage;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.PathValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Paths.ALL_PATHS;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;

public class UnsupportedStructTypesV1V2IT extends AbstractBoltTransportsTest
{
    private static final String USER_AGENT = "TestClient/1.0";

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings -> settings.put( auth_enabled.name(), "false" ) );

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();
    }

    @Test
    public void shouldFailWhenNullKeyIsSentWithInit() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );

        connection.send( util.chunk( 64, createMsgWithNullKey( BoltRequestMessage.INIT.signature() ) ) );

        assertThat( connection, util.eventuallyReceives(
                msgFailure( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailWhenDuplicateKeyIsSentWithInit() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );

        connection.send( util.chunk( 64, createMsgWithDuplicateKey( BoltRequestMessage.INIT.signature() ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Request.Invalid, "Duplicate map key `key1`." ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailWhenNullKeyIsSentWithRun() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsgWithNullKey( BoltRequestMessage.RUN.signature() ) ) );

        assertThat( connection, util.eventuallyReceives(
                msgFailure( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailWhenDuplicateKeyIsSentWithRun() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsgWithDuplicateKey( BoltRequestMessage.RUN.signature() ) ) );

        assertThat( connection, util.eventuallyReceives( msgFailure( Status.Request.Invalid, "Duplicate map key `key1`." ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailWhenNodeIsSentWithRun() throws Exception
    {
        testFailureWithV1Value( ALICE, "Node" );
    }

    @Test
    public void shouldFailWhenRelationshipIsSentWithRun() throws Exception
    {
        testFailureWithV1Value( ALICE_KNOWS_BOB, "Relationship" );
    }

    @Test
    public void shouldFailWhenPathIsSentWithRun() throws Exception
    {
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

    @Test
    public void shouldTerminateConnectionWhenUnknownMessageIsSent() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsg( (byte)'A' ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldTerminateConnectionWhenUnknownTypeIsSent() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsgWithUnknownValue( BoltRequestMessage.RUN.signature() ) ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    private void testFailureWithV1Value( AnyValue value, String description ) throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.chunk( init( USER_AGENT, Collections.emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWithV1Value( value ) ) );

        assertThat( connection,
                util.eventuallyReceives( msgFailure( Status.Statement.TypeError, description + " values cannot be unpacked with this version of bolt." ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    private byte[] createRunWithV1Value( AnyValue value ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 2, BoltRequestMessage.RUN.signature() );
        packer.pack( "RETURN $x" );
        packer.packMapHeader( 1 );
        packer.pack( "x" );
        packer.pack( value );

        return out.bytes();
    }

    private byte[] createMsg( byte signature ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 0, signature );

        return out.bytes();
    }

    private byte[] createMsgWithNullKey( byte signature ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 2, signature );
        packer.pack( "Text" );
        packMapWithNullKey( packer );

        return out.bytes();
    }

    private byte[] createMsgWithDuplicateKey( byte signature ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 2, signature );
        packer.pack( "Text" );
        packMapWithDuplicateKey( packer );

        return out.bytes();
    }

    private byte[] createMsgWithUnknownValue( byte signature ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 2, signature );
        packer.pack( "Text" );
        packer.packMapHeader( 1 );
        packer.pack( "key1" );
        packer.packStructHeader( 0, (byte)'A' );

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
