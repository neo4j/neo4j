/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.ndp.messaging.v1;

import java.io.IOException;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.ndp.messaging.NDPIOException;
import org.neo4j.ndp.messaging.v1.message.AcknowledgeFailureMessage;
import org.neo4j.ndp.messaging.v1.message.DiscardAllMessage;
import org.neo4j.ndp.messaging.v1.message.FailureMessage;
import org.neo4j.ndp.messaging.v1.message.IgnoredMessage;
import org.neo4j.ndp.messaging.v1.message.InitializeMessage;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.ndp.messaging.v1.message.PullAllMessage;
import org.neo4j.ndp.messaging.v1.message.RecordMessage;
import org.neo4j.ndp.messaging.v1.message.RunMessage;
import org.neo4j.ndp.messaging.v1.message.SuccessMessage;
import org.neo4j.ndp.runtime.spi.Record;
import org.neo4j.packstream.PackListItemType;
import org.neo4j.packstream.PackStream;

import static org.neo4j.ndp.runtime.internal.Neo4jError.codeFromString;
import static org.neo4j.ndp.runtime.spi.Records.record;

public class PackStreamMessageFormatV1 implements MessageFormat
{
    public static final int VERSION = 1;

    @Override
    public int version()
    {
        return VERSION;
    }

    public enum MessageType implements PackStream.StructType
    {
        INITIALIZE((byte) 0x01, InitializeMessage.class),
        ACK_FAILURE((byte) 0x0F, AcknowledgeFailureMessage.class),
        RUN((byte) 0x10, RunMessage.class),
        DISCARD_ALL((byte) 0x2F, DiscardAllMessage.class),
        PULL_ALL((byte) 0x3F, PullAllMessage.class),

        RECORD((byte) 0x71, RecordMessage.class),
        SUCCESS((byte) 0x70, SuccessMessage.class),
        IGNORED((byte) 0x7E, IgnoredMessage.class),
        FAILURE((byte) 0x7F, FailureMessage.class);

        public static MessageType fromSignature( byte signature )
        {
            for ( MessageType type : MessageType.values() )
            {
                if ( type.signature == signature )
                {
                    return type;
                }
            }
            throw new IllegalArgumentException( "Illegal type signature '" + signature + "'" );
        }

        private final byte signature;
        private final Class instanceClass;

        MessageType( byte signature, Class instanceClass )
        {
            this.signature = signature;
            this.instanceClass = instanceClass;
        }

        @Override
        public byte signature()
        {
            return signature;
        }

        @Override
        public Class instanceClass()
        {
            return instanceClass;
        }

    }

    public static class Writer implements MessageFormat.Writer
    {
        public static final MessageBoundaryHook NO_OP = new MessageBoundaryHook()
        {
            @Override
            public void onMessageComplete() throws IOException
            {

            }
        };

        private final Neo4jPack.Packer packer;
        private final MessageBoundaryHook onMessageComplete;

        /**
         * @param packer serializer to output channel
         * @param onMessageComplete invoked for each message, after it's done writing to the output
         */
        public Writer( Neo4jPack.Packer packer, MessageBoundaryHook onMessageComplete )
        {
            this.packer = packer;
            this.onMessageComplete = onMessageComplete;
        }

        @Override
        public Writer write( Message message ) throws IOException
        {
            message.dispatch( this );
            return this;
        }

        @Override
        public void handleRunMessage( String statement, Map<String,Object> params )
                throws IOException
        {
            packer.packStructHeader( 2, MessageType.RUN );
            packer.pack( statement );
            packer.packMap( params );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handlePullAllMessage()
                throws IOException
        {
            packer.packStructHeader( 0, MessageType.PULL_ALL );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleDiscardAllMessage()
                throws IOException
        {
            packer.packStructHeader( 0, MessageType.DISCARD_ALL );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleAckFailureMessage() throws IOException
        {
            packer.packStructHeader( 0, MessageType.ACK_FAILURE );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleRecordMessage( Record item )
                throws IOException
        {
            Object[] fields = item.fields();
            packer.packStructHeader( 1, MessageType.RECORD );
            packer.packListHeader( fields.length, PackListItemType.ANY );
            for ( Object field : fields )
            {
                packer.pack( field );
            }
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleSuccessMessage( Map<String,Object> metadata )
                throws IOException
        {
            packer.packStructHeader( 1, MessageType.SUCCESS );
            packer.packMap( metadata );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleFailureMessage( Status status, String message )
                throws IOException
        {
            packer.packStructHeader( 1, MessageType.FAILURE );
            packer.packMapHeader( 2 );

            packer.pack( "code" );
            packer.pack( status.code().serialize() );

            packer.pack( "message" );
            packer.pack( message );
            
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleIgnoredMessage() throws IOException
        {
            packer.packStructHeader( 0, MessageType.IGNORED );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleInitializeMessage( String clientName ) throws IOException
        {
            packer.packStructHeader( 1, MessageType.INITIALIZE );
            packer.pack( clientName );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void flush() throws IOException
        {
            packer.flush();
        }

    }

    public static class Reader implements MessageFormat.Reader
    {
        private final Neo4jPack.Unpacker unpacker;

        public Reader( Neo4jPack.Unpacker unpacker )
        {
            this.unpacker = unpacker;
        }

        @Override
        public boolean hasNext() throws IOException
        {
            return unpacker.hasNext();
        }

        /**
         * Parse a single message into the given consumer.
         */
        @Override
        public <E extends Exception> void read( MessageHandler<E> output ) throws IOException, E
        {
            unpacker.unpackStructHeader();
            byte signature = unpacker.unpackStructSignature();
            MessageType type;
            try
            {
                type = MessageType.fromSignature( signature );
            }
            catch ( IllegalArgumentException e )
            {
                throw new NDPIOException( Status.Request.Invalid,
                        "0x" + Integer.toHexString( signature ) +
                                " is not a valid message type." );
            }
            try {
                switch ( type )
                {
                case RUN:
                    unpackRunMessage( output );
                    break;
                case DISCARD_ALL:
                    unpackDiscardAllMessage( output );
                    break;
                case PULL_ALL:
                    unpackPullAllMessage( output );
                    break;
                case RECORD:
                    unpackRecordMessage( output );
                    break;
                case SUCCESS:
                    unpackSuccessMessage( output );
                    break;
                case FAILURE:
                    unpackFailureMessage( output );
                    break;
                case ACK_FAILURE:
                    unpackAckFailureMessage( output );
                    break;
                case IGNORED:
                    unpackIgnoredMessage( output );
                    break;
                case INITIALIZE:
                    unpackInitializeMessage( output );
                    break;
                }
            }
            catch( PackStream.PackStreamException e )
            {
                throw new NDPIOException( Status.Request.InvalidFormat,
                        "Unable to read " + type.name() + " message. " +
                        "Error was: " + e.getMessage(), e );
            }
        }

        private <E extends Exception> void unpackAckFailureMessage( MessageHandler<E> output )
                throws E
        {
            output.handleAckFailureMessage();
        }

        private <E extends Exception> void unpackSuccessMessage( MessageHandler<E> output )
                throws E, IOException
        {
            Map<String,Object> map = unpacker.unpackMap();
            output.handleSuccessMessage( map );
        }

        private <E extends Exception> void unpackFailureMessage( MessageHandler<E> output )
                throws E, IOException
        {
            Map<String,Object> map = unpacker.unpackMap();

            String codeStr = map.containsKey( "code" ) ?
                    (String) map.get( "code" ) :
                    Status.General.UnknownFailure.name();

            String msg = map.containsKey( "message" ) ?
                    (String) map.get( "message" ) :
                    "<No message supplied>";

            output.handleFailureMessage( codeFromString( codeStr ), msg );
        }

        private <E extends Exception> void unpackIgnoredMessage( MessageHandler<E> output )
                throws E
        {
            output.handleIgnoredMessage();
        }

        private <E extends Exception> void unpackRecordMessage( MessageHandler<E> output )
                throws E, IOException
        {
            long length = unpacker.unpackListHeader();
            PackListItemType type = unpacker.unpackListItemType();
            assert type == PackListItemType.ANY;
            final Object[] fields = new Object[(int) length];
            for ( int i = 0; i < length; i++ )
            {
                fields[i] = unpacker.unpack();
            }
            output.handleRecordMessage( record( fields ) );
        }

        private <E extends Exception> void unpackRunMessage( MessageHandler<E> output )
                throws E, IOException
        {
            String statement = unpacker.unpackText();
            Map<String,Object> params = unpacker.unpackMap();
            output.handleRunMessage( statement, params );
        }

        private <E extends Exception> void unpackDiscardAllMessage( MessageHandler<E> output )
                throws E, IOException
        {
            output.handleDiscardAllMessage();
        }

        private <E extends Exception> void unpackPullAllMessage( MessageHandler<E> output )
                throws E, IOException
        {
            output.handlePullAllMessage();
        }

        private <E extends Exception> void unpackInitializeMessage( MessageHandler<E> output ) throws IOException, E
        {
            String clientName = unpacker.unpackText();
            output.handleInitializeMessage( clientName );
        }

    }
}
