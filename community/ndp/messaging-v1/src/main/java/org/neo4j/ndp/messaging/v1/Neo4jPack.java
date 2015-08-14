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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.ndp.messaging.NDPIOException;
import org.neo4j.ndp.messaging.v1.infrastructure.UnboundRelationship;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueNode;
import org.neo4j.ndp.messaging.v1.infrastructure.ValuePath;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueRelationship;
import org.neo4j.ndp.messaging.v1.infrastructure.ValueUnboundRelationship;
import org.neo4j.packstream.ObjectPacker;
import org.neo4j.packstream.ObjectUnpacker;
import org.neo4j.packstream.PackInput;
import org.neo4j.packstream.PackListItemType;
import org.neo4j.packstream.PackOutput;
import org.neo4j.packstream.PackStream;
import org.neo4j.packstream.PackType;

/**
 * Extended PackStream packer and unpacker classes for working
 * with Neo4j-specific data types, represented as structures.
 */
public class Neo4jPack
{
    public static final Map<String, Object> EMPTY_STRING_OBJECT_MAP = new HashMap<>();

    /**
     * Enumeration of Neo4j-specific structure descriptors. The following
     * structures are implemented:
     *
     * - Node
     * - Relationship
     * - UnboundRelationship
     * - Path
     *
     */
    public enum StructType implements PackStream.StructType
    {
        NODE('N', ValueNode.class),
        RELATIONSHIP('R', ValueRelationship.class),
        UNBOUND_RELATIONSHIP('r', ValueUnboundRelationship.class),
        PATH('P', ValuePath.class);

        /**
         * Select a structure based on a Java class.
         *
         * @param type the class to use for selection
         * @return the associated structure descriptor
         */
        public static StructType fromClass( Class type )
        {
            if ( Node.class.isAssignableFrom( type ) )
            {
                return NODE;
            }
            else if ( Relationship.class.isAssignableFrom( type ) )
            {
                return RELATIONSHIP;
            }
            else if ( UnboundRelationship.class.isAssignableFrom( type ) )
            {
                return UNBOUND_RELATIONSHIP;
            }
            else if ( Path.class.isAssignableFrom( type ) )
            {
                return PATH;
            }
            else
            {
                throw new IllegalArgumentException(
                        "The class " + type.getName() + " does not have a Neo4jPack equivalent" );
            }
        }

        /**
         * Select a structure based on a signature byte.
         *
         * @param signature the signature to use for selection
         * @return the associated structure descriptor
         */
        public static StructType fromSignature( byte signature )
        {
            for ( StructType type : StructType.values() )
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

        StructType( char signature, Class instanceClass )
        {
            this.signature = (byte) signature;
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

    public static class Packer extends PackStream.Packer implements ObjectPacker
    {
        private IdentityPack.Packer identityPacker = new IdentityPack.Packer();
        private PathPack.Packer pathPacker = new PathPack.Packer();

        public Packer( PackOutput output )
        {
            super( output );
        }

        @Override
        public void pack( Object obj ) throws IOException
        {
            // Note: below uses instanceof for quick implementation, this should be swapped over
            // to a dedicated visitable type that the serializer can simply visit. This would
            // create explicit contract for what can be serialized and allow performant method
            // dispatch rather than if branching.
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
                List list = (List) obj;
                packListHeader( list.size(), PackListItemType.ANY );
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
                packListHeader( array.length, PackListItemType.INTEGER );
                for ( short item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof int[] )
            {
                int[] array = (int[]) obj;
                packListHeader( array.length, PackListItemType.INTEGER );
                for ( int item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof long[] )
            {
                long[] array = (long[]) obj;
                packListHeader( array.length, PackListItemType.INTEGER );
                for ( long item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof float[] )
            {
                float[] array = (float[]) obj;
                packListHeader( array.length, PackListItemType.FLOAT );
                for ( float item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof double[] )
            {
                double[] array = (double[]) obj;
                packListHeader( array.length, PackListItemType.FLOAT );
                for ( double item : array )
                {
                    pack( item );
                }
            }
            else if ( obj instanceof boolean[] )
            {
                boolean[] array = (boolean[]) obj;
                packListHeader( array.length, PackListItemType.BOOLEAN );
                for ( boolean item : array )
                {
                    pack( item );
                }
            }
            else if ( obj.getClass().isArray() )
            {
                Object[] array = (Object[]) obj;
                packListHeader( array.length, PackListItemType.ANY );
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
                throw new NDPIOException( Status.General.UnknownFailure,
                        "Unpackable value " + obj + " of type " + obj.getClass().getName() );
            }
        }

        public void packMap( Map<String, Object> map ) throws IOException
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
            Collection<String> collectedKeys = Iterables.toList( entity.getPropertyKeys() );
            packMapHeader( collectedKeys.size() );
            for ( String key : collectedKeys )
            {
                pack( key );
                pack( entity.getProperty( key ) );
            }
        }

        public void packNodeIdentity( long id ) throws IOException
        {
            identityPacker.packNodeIdentity( this, id );
        }

        public void packRelationshipIdentity( long id ) throws IOException
        {
            identityPacker.packRelationshipIdentity( this, id );
        }

    }

    public static class Unpacker extends PackStream.Unpacker implements ObjectUnpacker
    {
        private IdentityPack.Unpacker identityUnpacker = new IdentityPack.Unpacker();
        private PathPack.Unpacker pathUnpacker = new PathPack.Unpacker();

        public Unpacker( PackInput input )
        {
            super( input );
        }

        @Override
        public Object unpack() throws IOException
        {
            PackType valType = peekNextType();
            switch ( valType )
            {
                case NULL:
                    // still need to move past the null value
                    unpackNull();
                    return null;
                case BOOLEAN:
                    return unpackBoolean();
                case INTEGER:
                    return unpackLong();
                case FLOAT:
                    return unpackDouble();
                case TEXT:
                    return unpackText();
                case LIST:
                    return unpackList();
                case MAP:
                    return unpackMap();
                case STRUCT:
                {
                    unpackStructHeader();
                    StructType type = StructType.fromSignature(
                            unpackStructSignature() );
                    switch ( type )
                    {
                        case NODE:
                            return ValueNode.unpackFields( this );
                        case RELATIONSHIP:
                            return ValueRelationship.unpackFields( this );
                        case UNBOUND_RELATIONSHIP:
                            return ValueUnboundRelationship.unpackFields( this );
                        case PATH:
                            return pathUnpacker.unpackFields( this );
                    }
                }
                default:
                    throw new NDPIOException( Status.Request.InvalidFormat,
                            "Unknown value type: " + valType );
            }
        }

        public Object unpackList() throws IOException
        {
            int size = (int) unpackListHeader();
            PackListItemType itemType = unpackListItemType();
            Class instanceClass;
            if ( itemType.isStruct() )
            {
                instanceClass = StructType.fromSignature( itemType.markerByte() ).instanceClass();
            }
            else
            {
                instanceClass = itemType.instanceClass();
            }
            return unpackListItems( size, instanceClass );
        }

        private <T> List<T> unpackListItems( int size, Class<T> type ) throws IOException
        {
            List<T> items = new ArrayList<>( size );
            for ( int i = 0; i < size; i++ )
            {
                Object unpacked = unpack();
                try
                {
                    items.add( type.cast( unpacked ) );
                }
                catch ( ClassCastException e )
                {
                    throw new PackStream.TypeError(
                            "Cannot cast " + unpacked.getClass().getName() + " value " +
                                    unpacked.toString() + " to " + type.getName() );
                }
            }
            return items;
        }

        public Map<String, Object> unpackMap() throws IOException
        {
            int size = (int) unpackMapHeader();
            if ( size == 0 )
            {
                return Collections.emptyMap();
            }
            Map<String, Object> map = new HashMap<>( size, 1 );
            for ( int i = 0; i < size; i++ )
            {
                String key = unpackText();
                map.put( key, unpack() );
            }
            return map;
        }
        // TODO: combine these
        public Map<String, Object> unpackProperties() throws IOException
        {
            int numProps = (int) unpackMapHeader();
            Map<String, Object> map;
            if ( numProps > 0 )
            {
                map = new HashMap<>( numProps, 1 );
                for ( int j = 0; j < numProps; j++ )
                {
                    String key = unpackText();
                    Object val = unpack();
                    map.put( key, val );
                }
            }
            else
            {
                map = EMPTY_STRING_OBJECT_MAP;
            }
            return map;
        }

        public long unpackNodeIdentity() throws IOException
        {
            return identityUnpacker.unpackNodeIdentity( this );
        }

        public long unpackRelationshipIdentity() throws IOException
        {
            return identityUnpacker.unpackRelationshipIdentity( this );
        }

    }
}
