/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Entity;
import org.neo4j.driver.Node;
import org.neo4j.driver.Path;
import org.neo4j.driver.Relationship;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.SimpleNode;
import org.neo4j.driver.internal.SimplePath;
import org.neo4j.driver.internal.SimpleRelationship;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.packstream.PackStream;
import org.neo4j.packstream.PackType;

import static org.neo4j.driver.Values.value;

public class PackStreamMessageFormatV1 implements MessageFormat
{
    public static final String CONTENT_TYPE = "application/vnd.neo4j.v1+packstream";

    public final static byte MSG_ACK_FAILURE = 0x0F;
    public final static byte MSG_RUN = 0x10;
    public final static byte MSG_DISCARD_ALL = 0x2F;
    public final static byte MSG_PULL_ALL = 0x3F;

    public final static byte MSG_RECORD = 0x71;
    public final static byte MSG_SUCCESS = 0x70;
    public final static byte MSG_IGNORED = 0x7E;
    public final static byte MSG_FAILURE = 0x7F;

    public static final char NODE = 'N';
    public static final char RELATIONSHIP = 'R';
    public static final char PATH = 'P';

    private static final Map<String,Value> EMPTY_STRING_VALUE_MAP = new HashMap<>( 0 );

    @Override
    public MessageFormat.Writer newWriter()
    {
        return new Writer();
    }

    @Override
    public MessageFormat.Reader newReader()
    {
        return new Reader();
    }

    private static class Writer implements MessageFormat.Writer, MessageHandler
    {
        private final PackStream.Packer packer = new PackStream.Packer( 8192 );

        @Override
        public void handleRunMessage(
                String statement,
                Map<String,Value> parameters ) throws IOException
        {
            packer.packStructHeader( 2, (char) MSG_RUN );
            packer.pack( statement );
            packRawMap( parameters );
        }

        @Override
        public void handlePullAllMessage() throws IOException
        {
            packer.packStructHeader( 0, (char) MSG_PULL_ALL );
        }

        @Override
        public void handleDiscardAllMessage() throws IOException
        {
            packer.packStructHeader( 0, (char) MSG_DISCARD_ALL );
        }

        @Override
        public void handleAckFailureMessage() throws IOException
        {
            packer.packStructHeader( 0, (char) MSG_ACK_FAILURE );
        }

        @Override
        public void handleSuccessMessage( Map<String,Value> meta ) throws IOException
        {
            packer.packStructHeader( 1, (char) MSG_SUCCESS );
            packRawMap( meta );
        }

        @Override
        public void handleRecordMessage( Value[] fields ) throws IOException
        {
            packer.packStructHeader( 1, (char) MSG_RECORD );
            packer.packListHeader( fields.length );
            for ( Value field : fields )
            {
                packValue( field );
            }
        }

        @Override
        public void handleFailureMessage( String code, String message ) throws IOException
        {
            packer.packStructHeader( 1, (char) MSG_FAILURE );
            packer.packMapHeader( 2 );

            packer.pack( "code" );
            packValue( value( code ) );

            packer.pack( "message" );
            packValue( value( message ) );
        }

        @Override
        public void handleIgnoredMessage() throws IOException
        {
            packer.packStructHeader( 0, (char) MSG_IGNORED );
        }

        private void packRawMap( Map<String,Value> map ) throws IOException
        {
            if ( map == null || map.size() == 0 )
            {
                packer.packMapHeader( 0 );
                return;
            }
            packer.packMapHeader( map.size() );
            for ( Map.Entry<String,Value> entry : map.entrySet() )
            {
                packer.pack( entry.getKey() );
                packValue( entry.getValue() );
            }
        }

        private void packValue( Value value ) throws IOException
        {
            if ( value == null )
            {
                packer.packNull();
            }
            else if ( value.isBoolean() )
            {
                packer.pack( value.javaBoolean() );
            }
            else if ( value.isInteger() )
            {
                packer.pack( value.javaLong() );
            }
            else if ( value.isFloat() )
            {
                packer.pack( value.javaDouble() );
            }
            else if ( value.isText() )
            {
                packer.pack( value.javaString() );
            }
            else if ( value.isMap() )
            {
                packer.packMapHeader( (int) value.size() );
                for ( String s : value.keys() )
                {
                    packer.pack( s );
                    packValue( value.get( s ) );
                }
            }
            else if ( value.isList() )
            {
                packer.packListHeader( (int) value.size() );
                for ( Value item : value )
                {
                    packValue( item );
                }
            }
            else if ( value.isNode() )
            {
                Node node = value.asNode();
                packer.packStructHeader( 3, NODE );
                packer.pack( node.identity().toString() );

                Iterable<String> labels = node.labels();
                packer.packListHeader( Iterables.count( labels ) );
                for ( String label : labels )
                {
                    packer.pack( label );
                }

                Iterable<String> keys = node.propertyKeys();
                packer.packMapHeader( (int) value.size() );
                for ( String propKey : keys )
                {
                    packer.pack( propKey );
                    packValue( node.property( propKey ) );
                }
            }
            else if ( value.isRelationship() )
            {
                Relationship rel = value.asRelationship();
                packer.packStructHeader( 5, RELATIONSHIP );
                packer.pack( rel.identity().toString() );
                packer.pack( rel.start().toString() );
                packer.pack( rel.end().toString() );

                packer.pack( rel.type() );

                Iterable<String> keys = rel.propertyKeys();
                packer.packMapHeader( (int) value.size() );
                for ( String propKey : keys )
                {
                    packer.pack( propKey );
                    packValue( rel.property( propKey ) );
                }
            }
            else if ( value.isPath() )
            {
                Path path = value.asPath();
                packer.packStructHeader( 1, PATH );
                packer.packListHeader( (int) path.length() * 2 + 1 );
                packValue( value( path.start() ) );
                for ( Path.Segment seg : path )
                {
                    packValue( value( seg.relationship() ) );
                    packValue( value( seg.end() ) );
                }
            }
            else
            {
                throw new UnsupportedOperationException( "Unknown type: " + value );
            }
        }

        @Override
        public void flush() throws IOException
        {
            packer.flush();
        }

        @Override
        public void write( Message msg ) throws IOException
        {
            msg.dispatch( this );
        }

        @Override
        public MessageFormat.Writer reset( OutputStream channel )
        {
            packer.reset( Channels.newChannel( channel ) );
            return this;
        }
    }

    private static class Reader implements MessageFormat.Reader
    {
        private final PackStream.Unpacker unpacker = new PackStream.Unpacker( 8192 );

        @Override
        public boolean hasNext() throws IOException
        {
            return unpacker.hasNext();
        }

        /**
         * Parse a single message into the given consumer.
         */
        @Override
        public void read( MessageHandler handler ) throws IOException
        {
            unpacker.unpackStructHeader();
            int type = (int) unpacker.unpackStructSignature();
            switch ( type )
            {
            case MSG_RUN:
                unpackRunMessage( handler );
                break;
            case MSG_DISCARD_ALL:
                unpackDiscardAllMessage( handler );
                break;
            case MSG_PULL_ALL:
                unpackPullAllMessage( handler );
                break;
            case MSG_RECORD:
                unpackItemMessage( handler );
                break;
            case MSG_SUCCESS:
                unpackSuccessMessage( handler );
                break;
            case MSG_FAILURE:
                unpackFailureMessage( handler );
                break;
            case MSG_IGNORED:
                unpackIgnoredMessage( handler );
                break;
            default:
                throw new IOException( "Unknown message type: " + type );
            }
        }

        @Override
        public MessageFormat.Reader reset( InputStream channel ) throws IOException
        {
            this.unpacker.reset( Channels.newChannel( channel ) );
            return this;
        }

        private void unpackIgnoredMessage( MessageHandler output ) throws IOException
        {
            output.handleIgnoredMessage();
        }

        private void unpackFailureMessage( MessageHandler output ) throws IOException
        {
            Map<String,Value> params = unpackRawMap();
            String code = params.get( "code" ).javaString();
            String message = params.get( "message" ).javaString();
            output.handleFailureMessage( code, message );
        }

        private void unpackRunMessage( MessageHandler output ) throws IOException
        {
            String statement = unpacker.unpackString();
            Map<String,Value> params = unpackRawMap();
            output.handleRunMessage( statement, params );
        }

        private void unpackDiscardAllMessage( MessageHandler output ) throws IOException
        {
            output.handleDiscardAllMessage();
        }

        private void unpackPullAllMessage( MessageHandler output ) throws IOException
        {
            output.handlePullAllMessage();
        }

        private void unpackSuccessMessage( MessageHandler output ) throws IOException
        {
            Map<String,Value> map = unpackRawMap();
            output.handleSuccessMessage( map );
        }

        private void unpackItemMessage( MessageHandler output ) throws IOException
        {
            int fieldCount = (int) unpacker.unpackListHeader();
            Value[] fields = new Value[fieldCount];
            for ( int i = 0; i < fieldCount; i++ )
            {
                fields[i] = unpackValue();
            }
            output.handleRecordMessage( fields );
        }

        private Value unpackValue() throws IOException
        {
            PackType type = unpacker.peekNextType();
            switch ( type )
            {
            case BYTES:
                break;
            case NULL:
                return null;
            case BOOLEAN:
                return value( unpacker.unpackBoolean() );
            case INTEGER:
                return value( unpacker.unpackLong() );
            case FLOAT:
                return value( unpacker.unpackDouble() );
            case TEXT:
                return value( unpacker.unpackString() );
            case MAP:
            {
                int size = (int) unpacker.unpackMapHeader();
                Map<String,Value> map = new HashMap<>();
                for ( int j = 0; j < size; j++ )
                {
                    String key = unpacker.unpackString();
                    map.put( key, unpackValue() );
                }
                return new MapValue( map );
            }
            case LIST:
            {
                int size = (int) unpacker.unpackListHeader();
                Value[] vals = new Value[size];
                for ( int j = 0; j < size; j++ )
                {
                    vals[j] = unpackValue();
                }
                return new ListValue( vals );
            }
            case STRUCT:
            {
                long size = unpacker.unpackStructHeader();
                switch ( unpacker.unpackStructSignature() )
                {

                case NODE:
                {
                    ensureCorrectStructSize( "NODE", 3, size );
                    String urn = unpacker.unpackString();

                    int numLabels = (int) unpacker.unpackListHeader();
                    List<String> labels = new ArrayList<>( numLabels );
                    for ( int i = 0; i < numLabels; i++ )
                    {
                        labels.add( unpacker.unpackString() );
                    }

                    int numProps = (int) unpacker.unpackMapHeader();
                    Map<String,Value> props = new HashMap<>();
                    for ( int j = 0; j < numProps; j++ )
                    {
                        String key = unpacker.unpackString();
                        props.put( key, unpackValue() );
                    }

                    return new NodeValue( new SimpleNode( urn, labels, props ) );
                }
                case RELATIONSHIP:
                {
                    ensureCorrectStructSize( "RELATIONSHIP", 5, size );
                    String urn = unpacker.unpackString();
                    String startUrn = unpacker.unpackString();
                    String endUrn = unpacker.unpackString();
                    String relType = unpacker.unpackString();

                    int numProps = (int) unpacker.unpackMapHeader();
                    Map<String,Value> props = new HashMap<>();
                    for ( int j = 0; j < numProps; j++ )
                    {
                        String key = unpacker.unpackString();
                        Value val = unpackValue();
                        props.put( key, val );
                    }

                    return new RelationshipValue(
                            new SimpleRelationship( urn, startUrn, endUrn, relType, props ) );
                }
                case PATH:
                {
                    ensureCorrectStructSize( "PATH", 1, size );
                    int length = (int) unpacker.unpackListHeader();
                    Entity[] entities = new Entity[length];
                    for ( int i = 0; i < length; i++ )
                    {
                        Value entity = unpackValue();
                        if ( entity.isNode() )
                        {
                            entities[i] = entity.asNode();
                        }
                        else if ( entity.isRelationship() )
                        {
                            entities[i] = entity.asRelationship();
                        }
                        else
                        {
                            throw new RuntimeException( "Entity is neither a node nor a relationship - what gives??" );
                        }
                    }
                    return new PathValue( new SimplePath( entities ) );
                }
                }
            }
            }

            throw new IOException( "Unknown value type: " + type );
        }

        private void ensureCorrectStructSize( String structName, int expected, long actual )
        {
            if ( expected != actual )
            {
                throw new ClientException(
                        String.format( "Invalid message received, serialized %s structures should have %d fields, " +
                                       "received %s structure has %d fields.", structName, expected, structName,
                                actual ) );
            }
        }

        private Map<String,Value> unpackRawMap() throws IOException
        {
            int size = (int) unpacker.unpackMapHeader();
            if ( size == 0 )
            {
                return EMPTY_STRING_VALUE_MAP;
            }
            Map<String,Value> map = new HashMap<>( size );
            for ( int i = 0; i < size; i++ )
            {
                String key = unpacker.unpackString();
                map.put( key, unpackValue() );
            }
            return map;
        }

    }
}
