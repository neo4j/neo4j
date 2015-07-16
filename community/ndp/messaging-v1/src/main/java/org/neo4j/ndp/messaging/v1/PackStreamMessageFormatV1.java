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
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.packstream.PackStream;
import org.neo4j.ndp.runtime.spi.Record;

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

    public interface MessageTypes
    {
        byte MSG_INITIALIZE = 0x01;
        byte MSG_ACK_FAILURE = 0x0F;
        byte MSG_RUN = 0x10;
        byte MSG_DISCARD_ALL = 0x2F;
        byte MSG_PULL_ALL = 0x3F;

        byte MSG_RECORD = 0x71;
        byte MSG_SUCCESS = 0x70;
        byte MSG_IGNORED = 0x7E;
        byte MSG_FAILURE = 0x7F;
    }

    static String messageTypeName( int type )
    {
        switch( type )
        {
        case MessageTypes.MSG_ACK_FAILURE: return "MSG_ACK_FAILURE";
        case MessageTypes.MSG_RUN:         return "MSG_RUN";
        case MessageTypes.MSG_DISCARD_ALL: return "MSG_DISCARD_ALL";
        case MessageTypes.MSG_PULL_ALL:    return "MSG_PULL_ALL";
        case MessageTypes.MSG_RECORD:      return "MSG_RECORD";
        case MessageTypes.MSG_SUCCESS:     return "MSG_SUCCESS";
        case MessageTypes.MSG_IGNORED:     return "MSG_IGNORED";
        case MessageTypes.MSG_FAILURE:     return "MSG_FAILURE";
        default: return "0x" + Integer.toHexString(type);
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
            packer.packStructHeader( 2, MessageTypes.MSG_RUN );
            packer.pack( statement );
            packer.packRawMap( params );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handlePullAllMessage()
                throws IOException
        {
            packer.packStructHeader( 0, MessageTypes.MSG_PULL_ALL );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleDiscardAllMessage()
                throws IOException
        {
            packer.packStructHeader( 0, MessageTypes.MSG_DISCARD_ALL );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleAckFailureMessage() throws IOException
        {
            packer.packStructHeader( 0, MessageTypes.MSG_ACK_FAILURE );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleRecordMessage( Record item )
                throws IOException
        {
            Object[] fields = item.fields();
            packer.packStructHeader( 1, MessageTypes.MSG_RECORD );
            packer.packListHeader( fields.length );
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
            packer.packStructHeader( 1, MessageTypes.MSG_SUCCESS );
            packer.packRawMap( metadata );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleFailureMessage( Status status, String message )
                throws IOException
        {
            packer.packStructHeader( 1, MessageTypes.MSG_FAILURE );
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
            packer.packStructHeader( 0, MessageTypes.MSG_IGNORED );
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleInitializeMessage( String clientName ) throws IOException
        {
            packer.packStructHeader( 1, MessageTypes.MSG_INITIALIZE );
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
            try
            {
                unpacker.unpackStructHeader();
                int type = (int) unpacker.unpackLong();

                try
                {
                    switch ( type )
                    {
                    case MessageTypes.MSG_RUN:
                        unpackRunMessage( output );
                        break;
                    case MessageTypes.MSG_DISCARD_ALL:
                        unpackDiscardAllMessage( output );
                        break;
                    case MessageTypes.MSG_PULL_ALL:
                        unpackPullAllMessage( output );
                        break;
                    case MessageTypes.MSG_RECORD:
                        unpackRecordMessage( output );
                        break;
                    case MessageTypes.MSG_SUCCESS:
                        unpackSuccessMessage( output );
                        break;
                    case MessageTypes.MSG_FAILURE:
                        unpackFailureMessage( output );
                        break;
                    case MessageTypes.MSG_ACK_FAILURE:
                        unpackAckFailureMessage( output );
                        break;
                    case MessageTypes.MSG_IGNORED:
                        unpackIgnoredMessage( output );
                        break;
                    case MessageTypes.MSG_INITIALIZE:
                        unpackInitializeMessage( output );
                        break;
                    default:
                        throw new NDPIOException( Status.Request.Invalid,
                                "0x" + Integer.toHexString(type) + " is not a valid message type." );
                    }
                }
                catch( PackStream.PackStreamException e )
                {
                    throw new NDPIOException( Status.Request.InvalidFormat,
                            "Unable to read " + messageTypeName (type) + " message. " +
                            "Error was: " + e.getMessage(), e );
                }
            }
            catch( PackStream.PackStreamException e )
            {
                throw new NDPIOException( Status.Request.InvalidFormat, "Unable to read message type. " +
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
            Map<String,Object> map = unpacker.unpackRawMap();
            output.handleSuccessMessage( map );
        }

        private <E extends Exception> void unpackFailureMessage( MessageHandler<E> output )
                throws E, IOException
        {
            Map<String,Object> map = unpacker.unpackRawMap();

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
            Map<String,Object> params = unpacker.unpackRawMap();
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
