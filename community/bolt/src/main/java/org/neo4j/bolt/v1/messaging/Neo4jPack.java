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
package org.neo4j.bolt.v1.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.infrastructure.ValueNode;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueRelationship;
import org.neo4j.bolt.v1.messaging.infrastructure.ValueUnboundRelationship;
import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.bolt.v1.packstream.PackType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.bolt.v1.packstream.PackStream.UNKNOWN_SIZE;

/**
 * Extended PackStream packer and unpacker classes for working
 * with Neo4j-specific data types, represented as structures.
 */
public class Neo4jPack
{
    public static final List<Object> EMPTY_LIST = new ArrayList<>();
    public static final Map<String, Object> EMPTY_MAP = new HashMap<>();

    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte UNBOUND_RELATIONSHIP = 'r';
    public static final byte PATH = 'P';

    public static class Packer extends PackStream.Packer
    {
        private PathPack.Packer pathPacker = new PathPack.Packer();

        public Packer( PackOutput output )
        {
            super( output );
        }

        public void pack( Object obj ) throws IOException
        {
            // Note: below uses instanceof for quick implementation, this should be swapped over
            // to a dedicated
            // visitable type that the serializer can simply visit. This would create explicit
            // contract for what can
            // be serialized and allow performant method dispatch rather than if branching.
            if ( obj == null )
            {
                packNull();
            }
            else if ( obj instanceof Boolean )
            {
                pack( (boolean) obj );
            }
            else if ( obj instanceof Byte || obj instanceof Short || obj instanceof Integer ||
                    obj instanceof Long )
            {
                pack( ((Number) obj).longValue() );
            }
            else if ( obj instanceof Float || obj instanceof Double )
            {
                pack( ((Number) obj).doubleValue() );
            }
            else if ( obj instanceof String )
            {
                pack( (String) obj );
            }
            else if ( obj instanceof Map )
            {
                Map<Object, Object> map = (Map<Object, Object>) obj;

                packMapHeader( map.size() );
                for ( Map.Entry<?, ?> entry : map.entrySet() )
                {
                    pack( entry.getKey().toString() );
                    pack( entry.getValue() );
                }
            }
            else if ( obj instanceof Collection )
            {
                Collection list = (Collection) obj;
                packListHeader( list.size() );
                for ( Object item : list )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof byte[] )
            {
                // Pending decision
                throw new UnsupportedOperationException( "Binary values cannot be packed." );
            }
            else if ( obj instanceof short[] )
            {
                short[] array = (short[]) obj;
                packListHeader( array.length );
                for ( short item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof int[] )
            {
                int[] array = (int[]) obj;
                packListHeader( array.length );
                for ( int item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof long[] )
            {
                long[] array = (long[]) obj;
                packListHeader( array.length );
                for ( long item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof float[] )
            {
                float[] array = (float[]) obj;
                packListHeader( array.length );
                for ( float item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof double[] )
            {
                double[] array = (double[]) obj;
                packListHeader( array.length );
                for ( double item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof boolean[] )
            {
                boolean[] array = (boolean[]) obj;
                packListHeader( array.length );
                for ( boolean item : array )
                {
                    pack( item );
                }
            }
            else if ( obj.getClass().isArray() )
            {
                Object[] array = (Object[]) obj;
                packListHeader( array.length );
                for ( Object item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof Node )
            {
                ValueNode.pack( this, (Node) obj );
            }
            else if ( obj instanceof Relationship )
            {
                ValueRelationship.pack( this, (Relationship) obj );
            }
            else if ( obj instanceof Path )
            {
                pathPacker.pack( this, (Path) obj );
            }
            else
            {
                throw new BoltIOException( Status.General.UnknownFailure,
                        "Unpackable value " + obj + " of type " + obj.getClass().getName() );
            }
        }

        public void packRawMap( Map<String, Object> map ) throws IOException
        {
            packMapHeader( map.size() );
            if ( map.size() > 0 )
            {
                for ( Map.Entry<String, Object> entry : map.entrySet() )
                {
                    pack( entry.getKey() );
                    pack( entry.getValue() );
                }
            }
        }
        // TODO: combine these
        public void packProperties( PropertyContainer entity ) throws IOException
        {
            Map<String, Object> props = entity.getAllProperties();
            packMapHeader( props.size() );
            for ( Map.Entry<String, Object> property : props.entrySet() )
            {
                pack( property.getKey() );
                pack( property.getValue() );
            }
        }
    }


    public static class Unpacker extends PackStream.Unpacker
    {
        private PathPack.Unpacker pathUnpacker = new PathPack.Unpacker();

        public Unpacker( PackInput input )
        {
            super( input );
        }

        public Object unpack() throws IOException
        {
            PackType valType = peekNextType();
            switch ( valType )
            {
                case STRING:
                    return unpackString();
                case INTEGER:
                    return unpackLong();
                case FLOAT:
                    return unpackDouble();
                case BOOLEAN:
                    return unpackBoolean();
                case NULL:
                    // still need to move past the null value
                    unpackNull();
                    return null;
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
                            return ValueNode.unpackFields( this );
                        }
                        case RELATIONSHIP:
                        {
                            return ValueRelationship.unpackFields( this );
                        }
                        case UNBOUND_RELATIONSHIP:
                        {
                            return ValueUnboundRelationship.unpackFields( this );
                        }
                        case PATH:
                        {
                            return pathUnpacker.unpackFields( this );
                        }
                        default:
                            throw new BoltIOException( Status.Request.InvalidFormat,
                                    "Unknown struct type: " + Integer.toHexString(signature) );
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

        public List<Object> unpackList() throws IOException
        {
            int size = (int) unpackListHeader();
            if ( size == 0 )
            {
                return EMPTY_LIST;
            }
            ArrayList<Object> list;
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
            return list;
        }

        public Map<String, Object> unpackMap() throws IOException
        {
            int size = (int) unpackMapHeader();
            if ( size == 0 )
            {
                return EMPTY_MAP;
            }
            Map<String, Object> map;
            if ( size == UNKNOWN_SIZE ) {
                map = new HashMap<>();
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
                        case STRING:
                            String key = unpackString();
                            Object val = unpack();
                            if( map.put( key, val ) != null )
                            {
                                throw new BoltIOException( Status.Request.Invalid, "Duplicate map key `" + key + "`." );
                            }
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
                    String key = unpackString();
                    Object val = unpack();
                    if( map.put( key, val ) != null )
                    {
                        throw new BoltIOException( Status.Request.Invalid, "Duplicate map key `" + key + "`." );
                    }
                }
            }
            return map;
        }
    }
}
