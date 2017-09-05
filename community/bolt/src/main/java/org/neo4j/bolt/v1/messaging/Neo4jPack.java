/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.bolt.v1.packstream.PackType;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.collection.primitive.PrimitiveLongIntKeyValueArray;
import org.neo4j.helpers.BaseToObjectValueWriter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.CoordinateReferenceSystem;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.bolt.v1.packstream.PackStream.UNKNOWN_SIZE;
import static org.neo4j.values.storable.Values.byteArray;

/**
 * Extended PackStream packer and unpacker classes for working
 * with Neo4j-specific data types, represented as structures.
 */
public class Neo4jPack
{
    private static final List<Object> EMPTY_LIST = new ArrayList<>();
    private static final Map<String,AnyValue> EMPTY_MAP = new HashMap<>();

    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte UNBOUND_RELATIONSHIP = 'r';
    public static final byte PATH = 'P';

    public static class Packer extends PackStream.Packer implements AnyValueWriter<IOException>
    {
        private Error error;
        private static final int INITIAL_PATH_CAPACITY = 500;
        private static final int NO_SUCH_ID = -1;
        private final PrimitiveLongIntKeyValueArray nodeIndexes =
                new PrimitiveLongIntKeyValueArray( INITIAL_PATH_CAPACITY + 1 );
        private final PrimitiveLongIntKeyValueArray edgeIndexes =
                new PrimitiveLongIntKeyValueArray( INITIAL_PATH_CAPACITY );

        public Packer( PackOutput output )
        {
            super( output );
        }

        public void pack( AnyValue value ) throws IOException
        {
            value.writeTo( this );
        }

        public void packRawMap( MapValue map ) throws IOException
        {
            packMapHeader( map.size() );
            for ( Map.Entry<String,AnyValue> entry : map.entrySet() )
            {
                pack( entry.getKey() );
                pack( entry.getValue() );
            }
        }

        void consumeError() throws BoltIOException
        {
            if ( error != null )
            {
                BoltIOException exception = new BoltIOException( error.status(), error.msg() );
                error = null;
                throw exception;
            }
        }

        public boolean hasErrors()
        {
            return error != null;
        }

        @Override
        public void writeNodeReference( long nodeId ) throws IOException
        {
            packStructHeader( 3, Neo4jPack.NODE );
            pack( nodeId );
            packListHeader( 0 );
            packMapHeader( 0 );
        }

        @Override
        public void writeNode( long nodeId, TextArray labels, MapValue properties ) throws IOException
        {
            packStructHeader( 3, Neo4jPack.NODE );
            pack( nodeId );
            packListHeader( labels.length() );
            for ( int i = 0; i < labels.length(); i++ )
            {
                labels.value( i ).writeTo( this );
            }
            properties.writeTo( this );
        }

        @Override
        public void writeEdgeReference( long edgeId ) throws IOException
        {
            throw new UnsupportedOperationException( "Cannot write a raw edge reference" );
        }

        @Override
        public void writeEdge( long edgeId, long startNodeId, long endNodeId, TextValue type, MapValue properties )
                throws IOException
        {
            packStructHeader( 5, Neo4jPack.RELATIONSHIP );
            pack( edgeId );
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
        public void endMap() throws IOException
        {
            //do nothing
        }

        @Override
        public void beginList( int size ) throws IOException
        {
            packListHeader( size );
        }

        @Override
        public void endList() throws IOException
        {
            //do nothing
        }

        @Override
        public void writePath( NodeValue[] nodes, EdgeValue[] edges ) throws IOException
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
            packStructHeader( 3, Neo4jPack.PATH );

            writeNodesForPath( nodes );
            writeEdgesForPath( edges );

            packListHeader( 2 * edges.length );
            if ( edges.length == 0 )
            {
                return;
            }

            NodeValue node = nodes[0];
            for ( int i = 1; i <= 2 * edges.length; i++ )
            {
                if ( i % 2 == 0 )
                {
                    node = nodes[i / 2];
                    int index = nodeIndexes.getOrDefault( node.id(), NO_SUCH_ID );
                    pack( index );
                }
                else
                {
                    EdgeValue edge = edges[i / 2];
                    int index = edgeIndexes.getOrDefault( edge.id(), NO_SUCH_ID );

                    if ( node.id() == edge.startNode().id() )
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

        private void writeEdgesForPath( EdgeValue[] edges ) throws IOException
        {
            edgeIndexes.reset( edges.length );
            for ( EdgeValue node : edges )
            {
                // relationship indexes are one-based
                edgeIndexes.putIfAbsent( node.id(), edgeIndexes.size() + 1 );
            }

            int size = edgeIndexes.size();
            packListHeader( size );
            if ( size > 0 )
            {
                {
                    EdgeValue edge = edges[0];
                    for ( long id : edgeIndexes.keys() )
                    {
                        int i = 1;
                        while ( edge.id() != id )
                        {
                            edge = edges[i++];
                        }
                        //Note that we are not doing edge.writeTo(this) here since the serialization protocol
                        //requires these to be _unbound relationships_, thus edges without any start node nor
                        // end node.
                        packStructHeader( 3, Neo4jPack.UNBOUND_RELATIONSHIP );
                        pack( edge.id() );
                        edge.type().writeTo( this );
                        edge.properties().writeTo( this );
                    }
                }
            }
        }

        @Override
        public void beginPoint( CoordinateReferenceSystem coordinateReferenceSystem ) throws IOException
        {
            error = new Error( Status.Request.Invalid,
                    "Point is not yet supported as a return type in Bolt" );
            packNull();
        }

        @Override
        public void endPoint() throws IOException
        {
            //Do nothing
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
        public void writeString( char[] value, int offset, int length ) throws IOException
        {
            pack( String.valueOf( value, offset, length ) );
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
        public void endArray() throws IOException
        {
            //Do nothing
        }

        @Override
        public void writeByteArray( byte[] value ) throws IOException
        {
            pack( value );
        }
    }

    public static class Unpacker extends PackStream.Unpacker
    {

        private List<Neo4jError> errors = new ArrayList<>( 2 );

        public Unpacker( PackInput input )
        {
            super( input );
        }

        public AnyValue unpack() throws IOException
        {
            PackType valType = peekNextType();
            switch ( valType )
            {
            case BYTES:
                return byteArray( unpackBytes() );
            case STRING:
                return Values.stringValue( unpackString() );
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
                unpackStructHeader();
                char signature = unpackStructSignature();
                switch ( signature )
                {
                case NODE:
                {
                    throw new BoltIOException( Status.Request.Invalid, "Nodes cannot be unpacked." );
                }
                case RELATIONSHIP:
                {
                    throw new BoltIOException( Status.Request.Invalid, "Relationships cannot be unpacked." );
                }
                case UNBOUND_RELATIONSHIP:
                {
                    throw new BoltIOException( Status.Request.Invalid, "Relationships cannot be unpacked." );
                }
                case PATH:
                {
                    throw new BoltIOException( Status.Request.Invalid, "Paths cannot be unpacked." );
                }
                default:
                    throw new BoltIOException( Status.Request.InvalidFormat,
                            "Unknown struct type: " + Integer.toHexString( signature ) );
                }
            }
            case END_OF_STREAM:
            {
                unpackEndOfStream();
                return null;
            }
            default:
                throw new BoltIOException( Status.Request.InvalidFormat,
                        "Unknown value type: " + valType );
            }
        }

        ListValue unpackList() throws IOException
        {
            int size = (int) unpackListHeader();
            if ( size == 0 )
            {
                return VirtualValues.EMPTY_LIST;
            }
            ArrayList<AnyValue> list;
            if ( size == UNKNOWN_SIZE )
            {
                list = new ArrayList<>();
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
            }
            else
            {
                list = new ArrayList<>( size );
                for ( int i = 0; i < size; i++ )
                {
                    list.add( unpack() );
                }
            }
            return VirtualValues.list( list.toArray( new AnyValue[list.size()] ) );
        }

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
                            errors.add(
                                    Neo4jError.from( Status.Request.Invalid, "Duplicate map key `" + key + "`." ) );
                        }
                        break;
                    case NULL:
                        errors.add( Neo4jError.from( Status.Request.Invalid,
                                "Value `null` is not supported as key in maps, must be a non-nullable string." ) );
                        unpackNull();
                        val = unpack();
                        map.put( null, val );
                        break;
                    default:
                        throw new PackStream.PackStreamException( "Bad key type" );
                    }
                }
            }
            else
            {
                map = new HashMap<>( size, 1 );
                for ( int i = 0; i < size; i++ )
                {
                    PackType type = peekNextType();
                    String key;
                    switch ( type )
                    {
                    case NULL:
                        errors.add( Neo4jError.from( Status.Request.Invalid,
                                "Value `null` is not supported as key in maps, must be a non-nullable string." ) );
                        unpackNull();
                        key = null;
                        break;
                    case STRING:
                        key = unpackString();
                        break;
                    default:
                        throw new PackStream.PackStreamException( "Bad key type: " + type );
                    }

                    AnyValue val = unpack();
                    if ( map.put( key, val ) != null )
                    {
                        errors.add( Neo4jError.from( Status.Request.Invalid, "Duplicate map key `" + key + "`." ) );
                    }
                }
            }
            return VirtualValues.map( map );
        }

        public Map<String,Object> unpackToRawMap() throws IOException
        {
            MapValue mapValue = unpackMap();
            HashMap<String,Object> map = new HashMap<>( mapValue.size() );
            for ( Map.Entry<String,AnyValue> entry : mapValue.entrySet() )
            {
                UnpackerWriter unpackerWriter = new UnpackerWriter();
                entry.getValue().writeTo( unpackerWriter );
                map.put( entry.getKey(), unpackerWriter.value() );
            }
            return map;
        }

        Optional<Neo4jError> consumeError()
        {
            if ( errors.isEmpty() )
            {
                return Optional.empty();
            }
            else
            {
                Neo4jError combined = Neo4jError.combine( errors );
                errors.clear();
                return Optional.of( combined );
            }
        }
    }

    private static class Error
    {
        private final Status status;
        private final String msg;

        private Error( Status status, String msg )
        {
            this.status = status;
            this.msg = msg;
        }

        Status status()
        {
            return status;
        }

        String msg()
        {
            return msg;
        }
    }

    private Neo4jPack()
    {
    }

    private static class UnpackerWriter extends BaseToObjectValueWriter<RuntimeException>
    {

        @Override
        protected Node newNodeProxyById( long id )
        {
            throw new UnsupportedOperationException( "Cannot unpack nodes" );
        }

        @Override
        protected Relationship newRelationshipProxyById( long id )
        {
            throw new UnsupportedOperationException( "Cannot unpack relationships" );
        }

        @Override
        protected Point newGeographicPoint( double longitude, double latitude, String name, int code, String href )
        {
            throw new UnsupportedOperationException( "Cannot unpack points" );
        }

        @Override
        protected Point newCartesianPoint( double x, double y, String name, int code, String href )
        {
            throw new UnsupportedOperationException( "Cannot unpack points" );
        }
    }
}
