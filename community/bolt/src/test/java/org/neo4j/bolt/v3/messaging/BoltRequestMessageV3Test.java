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
package org.neo4j.bolt.v3.messaging;

import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.RecordingByteChannel;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.packstream.BufferedChannelOutput;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackedInputArray;
import org.neo4j.bolt.runtime.SynchronousBoltConnection;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.PullAllMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v3.messaging.response.RecordMessage;
import org.neo4j.common.HexPrinter;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.System.lineSeparator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.testing.MessageConditions.serialize;
import static org.neo4j.bolt.v3.BoltProtocolV3ComponentFactory.newNeo4jPack;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

class BoltRequestMessageV3Test
{
    private final Neo4jPack neo4jPack = newNeo4jPack();

    @Test
    void shouldHandleCommonMessages() throws Throwable
    {
        assertSerializes( new HelloMessage( map( "user_agent", "MyClient/1.0", "scheme", "basic" ) ) );
        assertSerializes( new RunMessage( "CREATE (n) RETURN åäö" ) );
        assertSerializes( DiscardAllMessage.INSTANCE );
        assertSerializes( PullAllMessage.INSTANCE );
        assertSerializes( new BeginMessage() );
        assertSerializes( COMMIT_MESSAGE );
        assertSerializes( ROLLBACK_MESSAGE );
        assertSerializes( ResetMessage.INSTANCE );
        assertSerializes( GOODBYE_MESSAGE );
    }

    @Test
    void shouldHandleParameterizedStatements() throws Throwable
    {
        // Given
        MapValue parameters = ValueUtils.asMapValue( map( "n", 12L ) );

        // When
        RunMessage msg = serializeAndDeserialize( new RunMessage( "asd", parameters ) );

        // Then
        MapValue params = msg.params();
        assertThat( params ).isEqualTo( parameters );
    }

    //"B1 71 91 B3 4E 0C 92 |84 55 73 65 72 | 86 42 61 6E\n61 6E 61 A284 6E 61 6D 65 83 42 6F 62 83 61 67\n65 0E"
    //"B1 71 91 B3 4E 0C 92 |86 42 61 6E 61 6E 61| 84 55\n73 65 72 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67\n65 0E
    @Test
    void shouldSerializeNode() throws Throwable
    {
        NodeValue nodeValue = nodeValue( 12L, stringArray( "User", "Banana" ), map( new String[]{"name", "age"},
                new AnyValue[]{stringValue( "Bob" ), intValue( 14 )} ) );
        assertThat( serialized( nodeValue ) ).isEqualTo(
                "B1 71 91 B3 4E 0C 92 84 55 73 65 72 86 42 61 6E" + lineSeparator() +
                "61 6E 61 A2 84 6E 61 6D 65 83 42 6F 62 83 61 67" + lineSeparator() +
                "65 0E" );
    }

    @Test
    void shouldSerializeRelationship() throws Throwable
    {
        RelationshipValue rel = relationshipValue( 12L,
                nodeValue( 1L, stringArray(), VirtualValues.EMPTY_MAP ),
                nodeValue( 2L, stringArray(), VirtualValues.EMPTY_MAP ),
                stringValue( "KNOWS" ), map( new String[]{"name", "age"},
                        new AnyValue[]{stringValue( "Bob" ), intValue( 14 )} ) );
        assertThat( serialized( rel ) ).isEqualTo(
                "B1 71 91 B5 52 0C 01 02 85 4B 4E 4F 57 53 A2 84" + lineSeparator() +
                "6E 61 6D 65 83 42 6F 62 83 61 67 65 0E" );
    }

    private String serialized( AnyValue object ) throws IOException
    {
        RecordMessage message = new RecordMessage( new AnyValue[]{object} );
        return HexPrinter.hex( serialize( neo4jPack, message ), 4, " " );
    }

    private void assertSerializes( RequestMessage msg ) throws Exception
    {
        assertThat( serializeAndDeserialize( msg ) ).isEqualTo( msg );
    }

    private <T extends RequestMessage> T serializeAndDeserialize( T msg ) throws Exception
    {
        RecordingByteChannel channel = new RecordingByteChannel();
        Neo4jPack.Packer packer = neo4jPack.newPacker( new BufferedChannelOutput( channel ) );
        BoltRequestMessageWriter writer = new BoltRequestMessageWriterV3( packer );

        writer.write( msg ).flush();
        channel.eof();

        return unpack( channel );
    }

    private <T extends RequestMessage> T unpack( RecordingByteChannel channel ) throws Exception
    {
        List<RequestMessage> messages = new ArrayList<>();
        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        doAnswer( (Answer<Void>) invocationOnMock ->
        {
            RequestMessage msg = invocationOnMock.getArgument( 0 );
            messages.add( msg );
            return null;
        } ).when( stateMachine ).process( any(), any() );
        BoltRequestMessageReader reader = new BoltRequestMessageReaderV3( new SynchronousBoltConnection( stateMachine ),
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
            throw new AssertionError( "Failed to unpack message, wire data was:\n" + serialized + '[' + bytes.length + "b]", e );
        }

        return (T) messages.get( 0 );
    }
}
