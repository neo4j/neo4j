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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.ndp.messaging.NDPIOException;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueNode;
import org.neo4j.ndp.messaging.v1.infrastructure.ValuePath;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueRelationship;
import org.neo4j.ndp.messaging.v1.message.Message;
import org.neo4j.packstream.PackStream;
import org.neo4j.packstream.PackType;
import org.neo4j.ndp.runtime.spi.Record;

import static org.neo4j.ndp.messaging.v1.infrastructure.ValueParser.parseId;
import static org.neo4j.ndp.runtime.internal.Neo4jError.codeFromString;
import static org.neo4j.ndp.runtime.spi.Records.record;

public class PackStreamMessageFormatV1 implements MessageFormat
{
    public static final int VERSION = 1;

    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte PATH = 'P';

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

        private final PackStream.Packer packer;
        private final MessageBoundaryHook onMessageComplete;

        /**
         * @param packer serializer to output channel
         * @param onMessageComplete invoked for each message, after it's done writing to the output
         */
        public Writer( PackStream.Packer packer, MessageBoundaryHook onMessageComplete )
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
            packRawMap( params );
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
                packValue( field );
            }
            onMessageComplete.onMessageComplete();
        }

        @Override
        public void handleSuccessMessage( Map<String,Object> metadata )
                throws IOException
        {
            packer.packStructHeader( 1, MessageTypes.MSG_SUCCESS );
            packRawMap( metadata );
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

        private void packRawMap( Map<String,Object> map ) throws IOException
        {
            packer.packMapHeader( map.size() );
            if ( map.size() > 0 )
            {
                for ( Map.Entry<String,Object> entry : map.entrySet() )
                {
                    packer.pack( entry.getKey() );
                    packValue( entry.getValue() );
                }
            }
        }

        private void packValue( Object obj ) throws IOException
        {
            // Note: below uses instanceof for quick implementation, this should be swapped over to a dedicated
            // visitable type that the serializer can simply visit. This would create explicit contract for what can
            // be serialized and allow performant method dispatch rather than if branching.
            if ( obj == null )
            {
                packer.packNull();
            }
            else if ( obj instanceof Boolean )
            {
                packer.pack( (boolean) obj );
            }
            else if ( obj instanceof Byte || obj instanceof Short || obj instanceof Integer || obj instanceof Long )
            {
                packer.pack( ((Number) obj).longValue() );
            }
            else if ( obj instanceof Float || obj instanceof Double )
            {
                packer.pack( ((Number) obj).doubleValue() );
            }
            else if ( obj instanceof String )
            {
                packer.pack( (String) obj );
            }
            else if ( obj instanceof Map )
            {
                Map<Object,Object> map = (Map<Object,Object>) obj;

                packer.packMapHeader( map.size() );
                for ( Map.Entry<?,?> entry : map.entrySet() )
                {
                    packer.pack( entry.getKey().toString() );
                    packValue( entry.getValue() );
                }
            }
            else if ( obj instanceof Collection )
            {
                List list = (List) obj;
                packer.packListHeader( list.size() );
                for ( Object item : list )
                {
                    packValue( item );
                }
            }
            else if ( obj instanceof byte[] )
            {
                // Pending decision
                throw new UnsupportedOperationException( "Binary values cannot be packed." );
            }
            else if ( obj instanceof short[] )
            {
                short[] arr = (short[]) obj;
                packer.packListHeader( arr.length );
                for ( int i = 0; i < arr.length; i++ )
                {
                    packer.pack( arr[i] );
                }
            }
            else if ( obj instanceof int[] )
            {
                int[] arr = (int[]) obj;
                packer.packListHeader( arr.length );
                for ( int i = 0; i < arr.length; i++ )
                {
                    packer.pack( arr[i] );
                }
            }
            else if ( obj instanceof long[] )
            {
                long[] arr = (long[]) obj;
                packer.packListHeader( arr.length );
                for ( int i = 0; i < arr.length; i++ )
                {
                    packer.pack( arr[i] );
                }
            }
            else if ( obj instanceof float[] )
            {
                float[] arr = (float[]) obj;
                packer.packListHeader( arr.length );
                for ( int i = 0; i < arr.length; i++ )
                {
                    packer.pack( arr[i] );
                }
            }
            else if ( obj instanceof double[] )
            {
                double[] arr = (double[]) obj;
                packer.packListHeader( arr.length );
                for ( int i = 0; i < arr.length; i++ )
                {
                    packer.pack( arr[i] );
                }
            }
            else if ( obj instanceof boolean[] )
            {
                boolean[] arr = (boolean[]) obj;
                packer.packListHeader( arr.length );
                for ( int i = 0; i < arr.length; i++ )
                {
                    packer.pack( arr[i] );
                }
            }
            else if ( obj.getClass().isArray() )
            {
                Object[] arr = (Object[]) obj;
                packer.packListHeader( arr.length );
                for ( int i = 0; i < arr.length; i++ )
                {
                    packValue( arr[i] );
                }
            }
            else if ( obj instanceof Node )
            {
                Node node = (Node) obj;
                packer.packStructHeader( 3, NODE );
                packer.pack( "node/" + node.getId() );

                Collection<Label> labels = Iterables.toList( node.getLabels() );
                packer.packListHeader( labels.size() );
                for ( Label label : labels )
                {
                    packer.pack( label.name() );
                }

                Collection<String> propertyKeys = Iterables.toList( node.getPropertyKeys() );
                packer.packMapHeader( propertyKeys.size() );
                for ( String propertyKey : propertyKeys )
                {
                    packer.pack( propertyKey );
                    packValue( node.getProperty( propertyKey ) );
                }
            }
            else if ( obj instanceof Relationship )
            {
                Relationship rel = (Relationship) obj;
                packer.packStructHeader( 5, RELATIONSHIP );
                packer.pack( "rel/" + rel.getId() );
                packer.pack( "node/" + rel.getStartNode().getId() );
                packer.pack( "node/" + rel.getEndNode().getId() );

                packer.pack( rel.getType().name() );

                Collection<String> propertyKeys = Iterables.toList( rel.getPropertyKeys() );
                packer.packMapHeader( propertyKeys.size() );
                for ( String propertyKey : propertyKeys )
                {
                    packer.pack( propertyKey );
                    packValue( rel.getProperty( propertyKey ) );
                }
            }
            else if ( obj instanceof Path )
            {
                Path path = (Path) obj;
                packer.packStructHeader( 1, PATH );
                packer.packListHeader( path.length() * 2 + 1 );
                for ( PropertyContainer pc : path )
                {
                    packValue( pc );
                }
            }
            else
            {
                throw new NDPIOException( Status.General.UnknownFailure, "Unpackable value " + obj + " of type " + obj.getClass().getName() );
            }
        }
    }

    public static class Reader implements MessageFormat.Reader
    {
        private final PackStream.Unpacker unpacker;

        public Reader( PackStream.Unpacker input )
        {
            unpacker = input;
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
                catch( PackStream.PackstreamException e )
                {
                    throw new NDPIOException( Status.Request.InvalidFormat,
                            "Unable to read " + messageTypeName (type) + " message. " +
                            "Error was: " + e.getMessage() );
                }
            }
            catch( PackStream.PackstreamException e )
            {
                throw new NDPIOException( Status.Request.InvalidFormat, "Unable to read message type. " +
                        "Error was: " + e.getMessage() );
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
            Map<String,Object> map = unpackRawMap();
            output.handleSuccessMessage( map );
        }

        private <E extends Exception> void unpackFailureMessage( MessageHandler<E> output )
                throws E, IOException
        {
            Map<String,Object> map = unpackRawMap();

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
                fields[i] = unpackValue();
            }
            output.handleRecordMessage( record( fields ) );
        }

        private <E extends Exception> void unpackRunMessage( MessageHandler<E> output )
                throws E, IOException
        {
            String statement = unpacker.unpackString();
            Map<String,Object> params = unpackRawMap();
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
            String clientName = unpacker.unpackString();
            output.handleInitializeMessage( clientName );
        }

        private Map<String,Object> unpackRawMap() throws IOException
        {
            int size = (int) unpacker.unpackMapHeader();
            if ( size == 0 )
            {
                return Collections.emptyMap();
            }
            Map<String,Object> map = new HashMap<>( size, 1 );
            for ( int i = 0; i < size; i++ )
            {
                String key = unpacker.unpackString();
                map.put( key, unpackValue() );
            }
            return map;
        }

        private Object unpackValue() throws IOException
        {
            PackType valType = unpacker.peekNextType();
            switch ( valType )
            {
            case TEXT:
                return unpacker.unpackString();
            case INTEGER:
                return unpacker.unpackLong();
            case FLOAT:
                return unpacker.unpackDouble();
            case BOOLEAN:
                return unpacker.unpackBoolean();
            case NULL:
                // still need to move past the null value
                unpacker.unpackNull();
                return null;
            case LIST:
            {
                int size = (int) unpacker.unpackListHeader();
                if ( size == 0 )
                {
                    return Collections.EMPTY_LIST;
                }
                ArrayList<Object> vals = new ArrayList<>( size );
                for ( int j = 0; j < size; j++ )
                {
                    vals.add( unpackValue() );
                }
                return vals;
            }
            case MAP:
            {
                int size = (int) unpacker.unpackMapHeader();
                if ( size == 0 )
                {
                    return Collections.EMPTY_MAP;
                }
                Map<String,Object> map = new HashMap<>( size, 1 );
                for ( int j = 0; j < size; j++ )
                {
                    String key = unpacker.unpackString();
                    Object val = unpackValue();
                    map.put( key, val );
                }
                return map;
            }
            case STRUCT:
            {
                unpacker.unpackStructHeader();
                char signature = unpacker.unpackStructSignature();
                switch ( signature )
                {
                case NODE:
                {
                    String urn = unpacker.unpackString();

                    int numLabels = (int) unpacker.unpackListHeader();
                    List<Label> labels;
                    if ( numLabels > 0 )
                    {
                        labels = new ArrayList<>( numLabels );
                        for ( int i = 0; i < numLabels; i++ )
                        {
                            labels.add( DynamicLabel.label( unpacker.unpackString() ) );
                        }
                    }
                    else
                    {
                        labels = Collections.emptyList();
                    }

                    Map<String,Object> props = unpackProperties();

                    return new ValueNode( parseId( urn ), labels, props );
                }
                case RELATIONSHIP:
                {
                    String urn = unpacker.unpackString();
                    String startUrn = unpacker.unpackString();
                    String endUrn = unpacker.unpackString();
                    String relTypeName = unpacker.unpackString();

                    Map<String,Object> props = unpackProperties();

                    long relId = parseId( urn );
                    long startNodeId = parseId( startUrn );
                    long endNodeId = parseId( endUrn );
                    RelationshipType relType = DynamicRelationshipType.withName( relTypeName );

                    return new ValueRelationship( relId, startNodeId, endNodeId, relType, props );
                }
                case PATH:
                {
                    int length = (int) unpacker.unpackListHeader();
                    // Note, this obviously assumes blindly that the client will send us paths of manageble sizes,
                    // opening
                    // the door for a bad client to make us allocate a ton of extra RAM. The assumption here is that
                    // the client has gone through a handshake and we trust her. That said, this is still wasteful, so
                    // look into more efficient ways to handle this if we ever take paths as input arguments.
                    PropertyContainer[] entities = new PropertyContainer[length];
                    for ( int i = 0; i < length; i++ )
                    {
                        entities[i] = (PropertyContainer) unpackValue();
                    }
                    return new ValuePath( entities );
                }
                default:
                    throw new NDPIOException( Status.Request.InvalidFormat, "Unknown struct type: " + signature );
                }
            }
            default:
                throw new NDPIOException( Status.Request.InvalidFormat, "Unknown value type: " + valType );
            }
        }

        private Map<String,Object> unpackProperties() throws IOException
        {
            int numProps = (int) unpacker.unpackMapHeader();
            Map<String,Object> map;
            if ( numProps > 0 )
            {
                map = new HashMap<>( numProps, 1 );
                for ( int j = 0; j < numProps; j++ )
                {
                    String key = unpacker.unpackString();
                    Object val = unpackValue();
                    map.put( key, val );
                }
            }
            else
            {
                map = Collections.EMPTY_MAP;
            }
            return map;
        }
    }
}
