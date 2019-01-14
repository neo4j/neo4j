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

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.messaging.StructType;
import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.bolt.v1.packstream.PackType;
import org.neo4j.collection.primitive.PrimitiveLongIntKeyValueArray;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.bolt.v1.packstream.PackStream.UNKNOWN_SIZE;
import static org.neo4j.values.storable.Values.byteArray;

/**
 * Extended PackStream packer and unpacker classes for working
 * with Neo4j-specific data types, represented as structures.
 */
public class Neo4jPackV1 implements Neo4jPack
{
    public static final long VERSION = 1;

    public static final byte NODE = 'N';
    public static final int NODE_SIZE = 3;

    public static final byte RELATIONSHIP = 'R';
    public static final int RELATIONSHIP_SIZE = 5;

    public static final byte UNBOUND_RELATIONSHIP = 'r';
    public static final int UNBOUND_RELATIONSHIP_SIZE = 3;

    public static final byte PATH = 'P';
    public static final int PATH_SIZE = 3;

    @Override
    public Neo4jPack.Packer newPacker( PackOutput output )
    {
        return new PackerV1( output );
    }

    @Override
    public Neo4jPack.Unpacker newUnpacker( PackInput input )
    {
        return new UnpackerV1( input );
    }

    @Override
    public long version()
    {
        return VERSION;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    protected static class PackerV1 extends PackStream.Packer implements AnyValueWriter<IOException>, Neo4jPack.Packer
    {
        private static final int INITIAL_PATH_CAPACITY = 500;
        private static final int NO_SUCH_ID = -1;
        private final PrimitiveLongIntKeyValueArray nodeIndexes =
                new PrimitiveLongIntKeyValueArray( INITIAL_PATH_CAPACITY + 1 );
        private final PrimitiveLongIntKeyValueArray relationshipIndexes =
                new PrimitiveLongIntKeyValueArray( INITIAL_PATH_CAPACITY );

        protected PackerV1( PackOutput output )
        {
            super( output );
        }

        @Override
        public void pack( AnyValue value ) throws IOException
        {
            value.writeTo( this );
        }

        @Override
        public void writeNodeReference( long nodeId )
        {
            throw new UnsupportedOperationException( "Cannot write a raw node reference" );
        }

        @Override
        public void writeNode( long nodeId, TextArray labels, MapValue properties ) throws IOException
        {
            packStructHeader( NODE_SIZE, NODE );
            pack( nodeId );
            packListHeader( labels.length() );
            for ( int i = 0; i < labels.length(); i++ )
            {
                labels.value( i ).writeTo( this );
            }
            properties.writeTo( this );
        }

        @Override
        public void writeRelationshipReference( long relationshipId )
        {
            throw new UnsupportedOperationException( "Cannot write a raw relationship reference" );
        }

        @Override
        public void writeRelationship( long relationshipId, long startNodeId, long endNodeId, TextValue type, MapValue properties )
                throws IOException
        {
            packStructHeader( RELATIONSHIP_SIZE, RELATIONSHIP );
            pack( relationshipId );
            pack( startNodeId );
            pack( endNodeId );
            type.writeTo( this );
            properties.writeTo( this );
        }

        @Override
        public void beginMap( int size ) throws IOException
        {
            packMapHeader( size );
        }

        @Override
        public void endMap()
        {
            //do nothing
        }

        @Override
        public void beginList( int size ) throws IOException
        {
            packListHeader( size );
        }

        @Override
        public void endList()
        {
            //do nothing
        }

        @Override
        public void writePath( NodeValue[] nodes, RelationshipValue[] relationships ) throws IOException
        {
            //A path is serialized in the following form
            // Given path: (a {id: 42})-[r1 {id: 10}]->(b {id: 43})<-[r1 {id: 11}]-(c {id: 44})
            //The serialization will look like:
            //
            // {
            //    [a, b, c]
            //    [r1, r2]
            //    [1, 1, -2, 2]
            // }
            // The first list contains all nodes where the first node (a) is guaranteed to be the start node of
            // the path
            // The second list contains all edges of the path
            // The third list defines the path order, where every other item specifies the offset into the
            // relationship and node list respectively. Since all paths is guaranteed to start with a 0, meaning
            // that
            // a is the start node in this case, those are excluded. So the first integer in the array refers to the
            // position
            // in the relationship array (1 indexed where sign denotes direction) and the second one refers to
            // the offset
            // into the
            // node list (zero indexed) and so on.
            packStructHeader( PATH_SIZE, PATH );

            writeNodesForPath( nodes );
            writeRelationshipsForPath( relationships );

            packListHeader( 2 * relationships.length );
            if ( relationships.length == 0 )
            {
                return;
            }

            NodeValue node = nodes[0];
            for ( int i = 1; i <= 2 * relationships.length; i++ )
            {
                if ( i % 2 == 0 )
                {
                    node = nodes[i / 2];
                    int index = nodeIndexes.getOrDefault( node.id(), NO_SUCH_ID );
                    pack( index );
                }
                else
                {
                    RelationshipValue r = relationships[i / 2];
                    int index = relationshipIndexes.getOrDefault( r.id(), NO_SUCH_ID );

                    if ( node.id() == r.startNode().id() )
                    {
                        pack( index );
                    }
                    else
                    {
                        pack( -index );
                    }
                }

            }
        }

        private void writeNodesForPath( NodeValue[] nodes ) throws IOException
        {
            nodeIndexes.reset( nodes.length );
            for ( NodeValue node : nodes )
            {
                nodeIndexes.putIfAbsent( node.id(), nodeIndexes.size() );
            }

            int size = nodeIndexes.size();
            packListHeader( size );
            if ( size > 0 )
            {
                NodeValue node = nodes[0];
                for ( long id : nodeIndexes.keys() )
                {
                    int i = 1;
                    while ( node.id() != id )
                    {
                        node = nodes[i++];
                    }
                    node.writeTo( this );
                }
            }
        }

        private void writeRelationshipsForPath( RelationshipValue[] relationships ) throws IOException
        {
            relationshipIndexes.reset( relationships.length );
            for ( RelationshipValue node : relationships )
            {
                // relationship indexes are one-based
                relationshipIndexes.putIfAbsent( node.id(), relationshipIndexes.size() + 1 );
            }

            int size = relationshipIndexes.size();
            packListHeader( size );
            if ( size > 0 )
            {
                {
                    RelationshipValue edge = relationships[0];
                    for ( long id : relationshipIndexes.keys() )
                    {
                        int i = 1;
                        while ( edge.id() != id )
                        {
                            edge = relationships[i++];
                        }
                        //Note that we are not doing relationship.writeTo(this) here since the serialization protocol
                        //requires these to be _unbound relationships_, thus relationships without any start node nor
                        // end node.
                        packStructHeader( UNBOUND_RELATIONSHIP_SIZE, UNBOUND_RELATIONSHIP );
                        pack( edge.id() );
                        edge.type().writeTo( this );
                        edge.properties().writeTo( this );
                    }
                }
            }
        }

        @Override
        public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws IOException
        {
            throwUnsupportedTypeError( "Point" );
        }

        @Override
        public void writeDuration( long months, long days, long seconds, int nanos ) throws IOException
        {
            throwUnsupportedTypeError( "Duration" );
        }

        @Override
        public void writeDate( LocalDate localDate ) throws IOException
        {
            throwUnsupportedTypeError( "Date" );
        }

        @Override
        public void writeLocalTime( LocalTime localTime ) throws IOException
        {
            throwUnsupportedTypeError( "LocalTime" );
        }

        @Override
        public void writeTime( OffsetTime offsetTime ) throws IOException
        {
            throwUnsupportedTypeError( "Time" );
        }

        @Override
        public void writeLocalDateTime( LocalDateTime localDateTime ) throws IOException
        {
            throwUnsupportedTypeError( "LocalDateTime" );
        }

        @Override
        public void writeDateTime( ZonedDateTime zonedDateTime ) throws IOException
        {
            throwUnsupportedTypeError( "DateTime" );
        }

        @Override
        public void writeNull() throws IOException
        {
            packNull();
        }

        @Override
        public void writeBoolean( boolean value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeInteger( byte value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeInteger( short value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeInteger( int value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeInteger( long value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeFloatingPoint( float value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeFloatingPoint( double value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeUTF8( byte[] bytes, int offset, int length ) throws IOException
        {
            packUTF8(bytes, offset, length);
        }

        @Override
        public void writeString( String value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void writeString( char value ) throws IOException
        {
            pack( value );
        }

        @Override
        public void beginArray( int size, ArrayType arrayType ) throws IOException
        {
            switch ( arrayType )
            {
            case BYTE:
                packBytesHeader( size );
                break;
            default:
                packListHeader( size );
            }

        }

        @Override
        public void endArray()
        {
            //Do nothing
        }

        @Override
        public void writeByteArray( byte[] value ) throws IOException
        {
            pack( value );
        }

        void throwUnsupportedTypeError( String type ) throws BoltIOException
        {
            throw new BoltIOException( Status.Request.Invalid, type + " is not supported as a return type in Bolt protocol version 1. " +
                                                               "Please make sure driver supports at least protocol version 2. " +
                                                               "Driver upgrade is most likely required." );
        }
    }

    protected static class UnpackerV1 extends PackStream.Unpacker implements Neo4jPack.Unpacker
    {
        protected UnpackerV1( PackInput input )
        {
            super( input );
        }

        @Override
        public AnyValue unpack() throws IOException
        {
            PackType valType = peekNextType();
            switch ( valType )
            {
            case BYTES:
                return byteArray( unpackBytes() );
            case STRING:
                return Values.utf8Value( unpackUTF8() );
            case INTEGER:
                return Values.longValue( unpackLong() );
            case FLOAT:
                return Values.doubleValue( unpackDouble() );
            case BOOLEAN:
                return Values.booleanValue( unpackBoolean() );
            case NULL:
                // still need to move past the null value
                unpackNull();
                return Values.NO_VALUE;
            case LIST:
            {
                return unpackList();
            }
            case MAP:
            {
                return unpackMap();
            }
            case STRUCT:
            {
                long size = unpackStructHeader();
                char signature = unpackStructSignature();
                return unpackStruct( signature, size );
            }
            case END_OF_STREAM:
            {
                unpackEndOfStream();
                return null;
            }
            default:
                throw new BoltIOException( Status.Request.InvalidFormat, "Unknown value type: " + valType );
            }
        }

        ListValue unpackList() throws IOException
        {
            int size = (int) unpackListHeader();
            if ( size == 0 )
            {
                return VirtualValues.EMPTY_LIST;
            }
            else if ( size == UNKNOWN_SIZE )
            {
                List<AnyValue> list = new ArrayList<>();
                boolean more = true;
                while ( more )
                {
                    PackType keyType = peekNextType();
                    switch ( keyType )
                    {
                    case END_OF_STREAM:
                        unpack();
                        more = false;
                        break;
                    default:
                        list.add( unpack() );
                    }
                }
                return VirtualValues.list( list.toArray( new AnyValue[0] ) );
            }
            else
            {
                AnyValue[] values = new AnyValue[size];
                for ( int i = 0; i < size; i++ )
                {
                    values[i] = unpack();
                }
                return VirtualValues.list( values );
            }
        }

        protected AnyValue unpackStruct( char signature, long size ) throws IOException
        {
            StructType structType = StructType.valueOf( signature );
            if ( structType == null )
            {
                throw new BoltIOException( Status.Request.InvalidFormat,
                        String.format( "Struct types of 0x%s are not recognized.", Integer.toHexString( signature ) ) );
            }

            throw new BoltIOException( Status.Statement.TypeError,
                    String.format( "%s values cannot be unpacked with this version of bolt.", structType.description() ) );
        }

        @Override
        public MapValue unpackMap() throws IOException
        {
            int size = (int) unpackMapHeader();
            if ( size == 0 )
            {
                return VirtualValues.EMPTY_MAP;
            }
            Map<String,AnyValue> map;
            if ( size == UNKNOWN_SIZE )
            {
                map = new HashMap<>();
                boolean more = true;
                while ( more )
                {
                    PackType keyType = peekNextType();
                    String key;
                    AnyValue val;
                    switch ( keyType )
                    {
                    case END_OF_STREAM:
                        unpack();
                        more = false;
                        break;
                    case STRING:
                        key = unpackString();
                        val = unpack();
                        if ( map.put( key, val ) != null )
                        {
                            throw new BoltIOException( Status.Request.Invalid, "Duplicate map key `" + key + "`." );
                        }
                        break;
                    case NULL:
                        throw new BoltIOException( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." );
                    default:
                        throw new BoltIOException( Status.Request.InvalidFormat, "Bad key type: " + keyType );
                    }
                }
            }
            else
            {
                map = new HashMap<>( size, 1 );
                for ( int i = 0; i < size; i++ )
                {
                    PackType keyType = peekNextType();
                    String key;
                    switch ( keyType )
                    {
                    case NULL:
                        throw new BoltIOException( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." );
                    case STRING:
                        key = unpackString();
                        break;
                    default:
                        throw new BoltIOException( Status.Request.InvalidFormat, "Bad key type: " + keyType );
                    }

                    AnyValue val = unpack();
                    if ( map.put( key, val ) != null )
                    {
                        throw new BoltIOException( Status.Request.Invalid, "Duplicate map key `" + key + "`." );
                    }
                }
            }
            return VirtualValues.map( map );
        }
    }
}
