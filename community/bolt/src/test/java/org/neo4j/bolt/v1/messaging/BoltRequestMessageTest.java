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
package org.neo4j.bolt.v1.messaging;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.SynchronousBoltConnection;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.bolt.v1.messaging.response.RecordMessage;
import org.neo4j.bolt.v1.packstream.BufferedChannelOutput;
import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.System.lineSeparator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;
import static org.neo4j.bolt.v1.runtime.spi.Records.record;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

public class BoltRequestMessageTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Neo4jPack neo4jPack = new Neo4jPackV1();

    @Test
    public void shouldHandleCommonMessages() throws Throwable
    {
        assertSerializes( new InitMessage( "MyClient/1.0", map( "scheme", "basic" ) ) );
        assertSerializes( AckFailureMessage.INSTANCE );
        assertSerializes( ResetMessage.INSTANCE );
        assertSerializes( new RunMessage( "CREATE (n) RETURN åäö" ) );
        assertSerializes( DiscardAllMessage.INSTANCE );
        assertSerializes( PullAllMessage.INSTANCE );
    }

    @Test
    public void shouldHandleParameterizedStatements() throws Throwable
    {
        // Given
        MapValue parameters = ValueUtils.asMapValue( map( "n", 12L ) );

        // When
        RunMessage msg = serializeAndDeserialize( new RunMessage( "asd", parameters ) );

        // Then
        MapValue params = msg.params();
        assertThat( params, equalTo( parameters ) );
    }

    //"B1 71 91 B3 4E 0C 92 |84 55 73 65 72 | 86 42 61 6E\n61 6E 61 A284 6E 61 6D 65 83 42 6F 62 83 61 67\n65 0E"
    //"B1 71 91 B3 4E 0C 92 |86 42 61 6E 61 6E 61| 84 55\n73 65 72 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67\n65 0E
    @Test
    public void shouldSerializeNode() throws Throwable
    {
        NodeValue nodeValue = nodeValue( 12L, stringArray( "User", "Banana" ), map( new String[]{"name", "age"},
                new AnyValue[]{stringValue( "Bob" ), intValue( 14 )} ) );
        assertThat( serialized( nodeValue ),
                equalTo( "B1 71 91 B3 4E 0C 92 84 55 73 65 72 86 42 61 6E" + lineSeparator() +
                         "61 6E 61 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67" + lineSeparator() +
                         "65 0E" ) );
    }

    @Test
    public void shouldSerializeRelationship() throws Throwable
    {
        RelationshipValue rel = relationshipValue( 12L,
                nodeValue( 1L, stringArray(), VirtualValues.EMPTY_MAP ),
                nodeValue( 2L, stringArray(), VirtualValues.EMPTY_MAP ),
                stringValue( "KNOWS" ), map( new String[]{"name", "age"},
                        new AnyValue[]{stringValue( "Bob" ), intValue( 14 )} ) );
        assertThat( serialized( rel ),
                equalTo( "B1 71 91 B5 52 0C 01 02 85 4B 4E 4F 57 53 A2 84" + lineSeparator() +
                         "6E 61 6D 65 83 42 6F 62 83 61 67 65 0E" ) );
    }

    private String serialized( AnyValue object ) throws IOException
    {
        RecordMessage message = new RecordMessage( record( object ) );
        return HexPrinter.hex( serialize( neo4jPack, message ), 4, " " );
    }

    private void assertSerializes( RequestMessage msg ) throws Exception
    {
        assertThat( serializeAndDeserialize( msg ), equalTo( msg ) );
    }

    private <T extends RequestMessage> T serializeAndDeserialize( T msg ) throws Exception
    {
        RecordingByteChannel channel = new RecordingByteChannel();
        Neo4jPack.Packer packer = neo4jPack.newPacker( new BufferedChannelOutput( channel ) );
        BoltRequestMessageWriter writer = new BoltRequestMessageWriter( packer );

        writer.write( msg ).flush();
        channel.eof();

        return unpack( channel );
    }

    private <T extends RequestMessage> T unpack( RecordingByteChannel channel ) throws Exception
    {
        List<RequestMessage> messages = new ArrayList<>();
        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                RequestMessage msg = invocationOnMock.getArgument( 0 );
                messages.add( msg );
                return null;
            }
        } ).when( stateMachine ).process( any(), any() );
        BoltRequestMessageReader reader = new BoltRequestMessageReaderV1( new SynchronousBoltConnection( stateMachine ),
                mock( BoltResponseMessageWriter.class ), NullLogService.getInstance() );

        byte[] bytes = channel.getBytes();
        String serialized = HexPrinter.hex( bytes );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( new PackedInputArray( bytes ) );
        try
        {
            reader.read( unpacker );
        }
        catch ( Throwable e )
        {
            throw new AssertionError( "Failed to unpack message, wire data was:\n" + serialized + "[" + bytes.length + "b]", e );
        }

        return (T) messages.get( 0 );
    }
}
