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
package org.neo4j.bolt.transport;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackedOutputArray;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.PathValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.packstream.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.packstream.example.Nodes.ALICE;
import static org.neo4j.bolt.packstream.example.Paths.ALL_PATHS;
import static org.neo4j.bolt.testing.MessageMatchers.msgFailure;
import static org.neo4j.bolt.testing.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;

public class UnsupportedStructTypesV1V2IT extends AbstractBoltTransportsTest
{
    public static final byte DEFAULT_SIGNATURE = RunMessage.SIGNATURE;

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getSettingsFunction() );

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();
    }

    @Test
    public void shouldFailWhenNullKeyIsSent() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsgWithNullKey() ) );

        assertThat( connection, util.eventuallyReceives(
                msgFailure( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." ) ) );
        assertThat( connection, eventuallyDisconnects() );
    }

    @Test
    public void shouldFailWhenDuplicateKey() throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createMsgWithDuplicateKey() ) );

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
        connection.send( util.defaultAuth() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createUnknownMsg() ) );

        assertThat( connection, eventuallyDisconnects() );
    }

    private void testFailureWithV1Value( AnyValue value, String description ) throws Exception
    {
        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
        connection.send( util.defaultAuth() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( 64, createRunWithUnknownValue( value ) ) );

        assertThat( connection,
                util.eventuallyReceives( msgFailure( Status.Statement.TypeError, description + " values cannot be unpacked with this version of bolt." ) ) );
        assertThat( connection, eventuallyDisconnects() );
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
