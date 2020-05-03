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
package org.neo4j.bolt.testing;

import org.apache.commons.lang3.mutable.MutableObject;
import org.assertj.core.api.Condition;
import org.eclipse.jetty.websocket.api.WebSocketException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.ResponseMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.v3.messaging.response.RecordMessage;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.bolt.v41.BoltProtocolV41;
import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.responseMessage;
import static org.neo4j.bolt.testing.MessageConditions.serialize;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.DB_NAME_KEY;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class TransportTestUtil
{
    private static final BoltProtocolVersion DEFAULT_BOLT_VERSION = BoltProtocolV41.VERSION;
    protected final Neo4jPack neo4jPack;
    private final MessageEncoder messageEncoder;

    public TransportTestUtil()
    {
        this( new MessageEncoderV4() );
    }

    public TransportTestUtil( MessageEncoder messageEncoder )
    {
        this( new Neo4jPackV2(), messageEncoder );
    }

    public TransportTestUtil( Neo4jPack neo4jPack )
    {
        this( neo4jPack, new MessageEncoderV4() );
    }

    private TransportTestUtil( Neo4jPack neo4jPack, MessageEncoder messageEncoder )
    {
        this.neo4jPack = neo4jPack;
        this.messageEncoder = messageEncoder;
    }

    public Neo4jPack getNeo4jPack()
    {
        return neo4jPack;
    }

    public byte[] chunk( RequestMessage... messages ) throws IOException
    {
        return chunk( 32, messages );
    }

    public byte[] chunk( ResponseMessage... messages ) throws IOException
    {
        return chunk( 32, messages );
    }

    public byte[] chunk( int chunkSize, RequestMessage... messages ) throws IOException
    {
        byte[][] serializedMessages = new byte[messages.length][];
        for ( int i = 0; i < messages.length; i++ )
        {
            serializedMessages[i] = messageEncoder.encode( neo4jPack, messages[i] );
        }
        return chunk( chunkSize, serializedMessages );
    }

    public byte[] chunk( int chunkSize, ResponseMessage... messages ) throws IOException
    {
        byte[][] serializedMessages = new byte[messages.length][];
        for ( int i = 0; i < messages.length; i++ )
        {
            serializedMessages[i] = serialize( neo4jPack, messages[i] );
        }
        return chunk( chunkSize, serializedMessages );
    }

    public byte[] chunk( int chunkSize, byte[]... messages )
    {
        ByteBuffer output = ByteBuffers.allocate( 10000, INSTANCE );

        for ( byte[] wholeMessage : messages )
        {
            int left = wholeMessage.length;
            while ( left > 0 )
            {
                int size = Math.min( left, chunkSize );
                output.putShort( (short) size );

                int offset = wholeMessage.length - left;
                output.put( wholeMessage, offset, size );

                left -= size;
            }
            output.putShort( (short) 0 );
        }

        output.flip();

        byte[] arrayOutput = new byte[output.limit()];
        output.get( arrayOutput );
        return arrayOutput;
    }

    public byte[] defaultAcceptedVersions()
    {
        return acceptedVersions( DEFAULT_BOLT_VERSION.toInt(), 0, 0, 0 );
    }

    /**
     * Use this when you actually do not need to auth and just want bolt to ready to work
     */
    public byte[] defaultAuth() throws IOException
    {
        return chunk( BoltV4Messages.hello() );
    }

    public byte[] defaultAuth( Map<String,Object> authToken ) throws IOException
    {
        return chunk( BoltV4Messages.hello( authToken ) );
    }

    public byte[] defaultRunAutoCommitTx( String statement ) throws IOException
    {
        return chunk( BoltV4Messages.run( statement ), BoltV4Messages.pullAll() );
    }

    public byte[] defaultRunAutoCommitTxWithoutResult( String statement ) throws IOException
    {
        return chunk( BoltV4Messages.run( statement ), BoltV4Messages.discardAll() );
    }

    public byte[] defaultRunAutoCommitTx( String statement, MapValue params ) throws IOException
    {
        return chunk( BoltV4Messages.run( statement, params ), BoltV4Messages.pullAll() );
    }

    public byte[] defaultRunAutoCommitTx( String statement, MapValue params, String database ) throws IOException
    {
        var meta = VirtualValues.map( new String[]{DB_NAME_KEY}, new AnyValue[]{Values.stringValue( database )} );
        return chunk( new RunMessage( statement, params, meta, List.of(), null, AccessMode.WRITE, Map.of(), database ), BoltV4Messages.pullAll() );
    }

    public byte[] defaultRunAutoCommitTxWithoutResult( String statement, MapValue params ) throws IOException
    {
        return chunk( BoltV4Messages.run( statement, params ), BoltV4Messages.discardAll() );
    }

    public byte[] defaultRunExplicitCommitTxAndCommit( String statement ) throws IOException
    {
        return chunk( BoltV4Messages.begin(), BoltV4Messages.run( statement ), BoltV4Messages.pullAll(), BoltV4Messages.commit() );
    }

    public byte[] defaultRunExplicitCommitTxAndRollBack( String statement ) throws IOException
    {
        return chunk( BoltV4Messages.begin(), BoltV4Messages.run( statement ), BoltV4Messages.pullAll(), BoltV4Messages.rollback() );
    }

    public byte[] defaultReset() throws IOException
    {
        return chunk( BoltV4Messages.reset() );
    }

    public byte[] acceptedVersions( long option1, long option2, long option3, long option4 )
    {
        ByteBuffer bb = ByteBuffers.allocate( 5 * Integer.BYTES, INSTANCE );
        bb.putInt( 0x6060B017 );
        bb.putInt( (int) option1 );
        bb.putInt( (int) option2 );
        bb.putInt( (int) option3 );
        bb.putInt( (int) option4 );
        return bb.array();
    }

    @SafeVarargs
    public final <T extends TransportConnection> Consumer<T> eventuallyReceives( Consumer<ResponseMessage>... messagesConsumers )
    {
        return eventuallyReceives( false, () -> {}, messagesConsumers );
    }

    @SafeVarargs
    public final <T extends TransportConnection> Consumer<T> eventuallyReceives( boolean allowNoOp,
            Runnable noOpCallback, Consumer<ResponseMessage>... messagesConsumers )
    {
        return connection ->
        {
            try
            {
                for ( Consumer<ResponseMessage> messageCondition : messagesConsumers )
                {
                    var message = receiveOneResponseMessage( allowNoOp, noOpCallback, connection );
                    assertThat( message ).satisfies( messageCondition );
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Messages[" + Arrays.toString( messagesConsumers ) + "]", e );
            }
        };
    }

    @SafeVarargs
    public final <T extends TransportConnection> Consumer<T> eventuallyReceives( int skip,
            Consumer<ResponseMessage>... messagesConsumers )
    {
        return connection ->
        {
            try
            {
                for ( int i = 0; i < skip; i++ )
                {
                    var message = receiveOneResponseMessage( connection );
                    // we skip all record messages as it is not really a reply to a request message
                    while ( message instanceof RecordMessage )
                    {
                        message = receiveOneResponseMessage( connection );
                    }
                }
                for ( Consumer<ResponseMessage> messageCondition : messagesConsumers )
                {
                    var message = receiveOneResponseMessage( connection );
                    assertThat( message ).satisfies( messageCondition );
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Messages[" + Arrays.toString( messagesConsumers ) + "]", e );
            }
        };
    }

    @SafeVarargs
    public final <T extends TransportConnection> Consumer<T> eventuallyReceivesWithOptionalPrecedingMessages(
            final Pair<Consumer<ResponseMessage>,ResponseMatcherOptionality>... messages )
    {
        return transportConnection ->
        {
            try
            {
                // Sanity check
                if ( messages.length > 0 )
                {
                    assertThat( messages[messages.length - 1].other() )
                            .as("The last message matcher must be REQUIRED" )
                            .isEqualTo( ResponseMatcherOptionality.REQUIRED );
                }

                ResponseMessage message = null;
                for ( Pair<Consumer<ResponseMessage>, ResponseMatcherOptionality> matchesOptionalMessage : messages )
                {
                    if ( message == null )
                    {
                        message = receiveOneResponseMessage( transportConnection );
                    }

                    if ( matchesOptionalMessage.other() == ResponseMatcherOptionality.OPTIONAL )
                    {
                        try
                        {
                            assertThat( message ).satisfies( matchesOptionalMessage.first() );
                            message = null;
                        }
                        catch ( AssertionError e )
                        {
                            // else we will reuse the message to feed into the next matcher
                        }
                    }
                    else // if ( matchesOptionalMessage.other() == ResponseMatcherOptionality.REQUIRED )
                    {
                        assertThat( message ).satisfies( matchesOptionalMessage.first() );
                        message = null;
                    }
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    public static Condition<TransportConnection> eventuallyDisconnects()
    {
        return new Condition<>( connection ->
        {
                BooleanSupplier condition = () ->
                {
                    try
                    {
                        connection.send( new byte[]{0, 0} );
                        connection.recv( 1 );
                    }
                    catch ( IOException | WebSocketException e )
                    {
                        return true;
                    }
                    catch ( Exception e )
                    {
                        return false;
                    }
                    return false;
                };
                try
                {
                    Predicates.await( condition, 2, TimeUnit.SECONDS );
                    return true;
                }
                catch ( Exception e )
                {
                    return false;
                }
        }, "Eventually Disconnects" );
    }

    public static Condition<TransportConnection> serverImmediatelyDisconnects()
    {
        return new Condition<>( connection ->
        {
            try
            {
                connection.recv( 1 );
            }
            catch ( Exception e )
            {
                // take an IOException on send/receive as evidence of disconnection
                return e instanceof IOException;
            }
            return false;
        }, "Eventually Disconnects" );
    }

    public enum ResponseMatcherOptionality
    {
        REQUIRED,
        OPTIONAL
    }

    public Condition<TransportConnection> eventuallyReceivesSelectedProtocolVersion()
    {
        return eventuallyReceives( new byte[]{0, 0, (byte) DEFAULT_BOLT_VERSION.getMinorVersion(), (byte) DEFAULT_BOLT_VERSION.getMajorVersion()} );
    }

    public static Condition<TransportConnection> eventuallyReceives( final byte[] expected )
    {
        var arrayReference = new MutableObject<>();
        return new Condition<>( item -> {
            try
            {
                byte[] received = item.recv( expected.length );
                arrayReference.setValue( received );
                return Arrays.equals( received, expected );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }, "Eventually receive. Expected: " + Arrays.toString( expected ) +
                ", actual: " + Arrays.toString( (byte[]) arrayReference.getValue() ) );
    }

    public <T extends TransportConnection> ResponseMessage receiveOneResponseMessage( T conn ) throws IOException,
            InterruptedException
    {
        return receiveOneResponseMessage( false, () -> {}, conn );
    }

    public <T extends TransportConnection> ResponseMessage receiveOneResponseMessage( boolean allowNoOp,
            Runnable noOpCallback, T conn )
            throws IOException, InterruptedException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        while ( true )
        {
            int size = receiveChunkHeader( conn );
            while ( allowNoOp && size == 0 )
            {
                size = receiveChunkHeader( conn );
                noOpCallback.run();
            }

            if ( size > 0 )
            {
                byte[] received = conn.recv( size );
                bytes.write( received );
                // Once this message started, then there should never be a NOOP
                allowNoOp = false;
            }
            else
            {
                return responseMessage( neo4jPack, bytes.toByteArray() );
            }
        }
    }

    public int receiveChunkHeader( TransportConnection conn ) throws IOException, InterruptedException
    {
        byte[] raw = conn.recv( 2 );
        return ((raw[0] & 0xff) << 8 | (raw[1] & 0xff)) & 0xffff;
    }

    public interface MessageEncoder
    {
        byte[] encode( Neo4jPack neo4jPack, RequestMessage... messages ) throws IOException;
        byte[] encode( Neo4jPack neo4jPack, ResponseMessage... messages ) throws IOException;
    }

    private static class MessageEncoderV4 implements MessageEncoder
    {
        @Override
        public byte[] encode( Neo4jPack neo4jPack, RequestMessage... messages ) throws IOException
        {
            return serialize( neo4jPack, messages );
        }

        @Override
        public byte[] encode( Neo4jPack neo4jPack, ResponseMessage... messages ) throws IOException
        {
            return serialize( neo4jPack, messages );
        }
    }
}
