/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.neo4j.function.Supplier;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.store.record.PropertyBlock;

/**
 * Defines valid property types.
 */
@SuppressWarnings("UnnecessaryBoxing")
public enum PropertyType
{
    BOOL( 1 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.booleanProperty( propertyKeyId, getValue( block.getSingleValueLong() ) );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return getValue( block.getSingleValueLong() );
        }

        private boolean getValue( long propBlock )
        {
            return ( propBlock & 0x1 ) == 1;
        }
    },
    BYTE( 2 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.byteProperty( propertyKeyId, block.getSingleValueByte() );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Byte.valueOf( block.getSingleValueByte() );
        }
    },
    SHORT( 3 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.shortProperty( propertyKeyId, block.getSingleValueShort() );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Short.valueOf( block.getSingleValueShort() );
        }
    },
    CHAR( 4 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.charProperty( propertyKeyId, (char) block.getSingleValueShort() );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Character.valueOf( (char) block.getSingleValueShort() );
        }
    },
    INT( 5 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.intProperty( propertyKeyId, block.getSingleValueInt() );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Integer.valueOf( block.getSingleValueInt() );
        }
    },
    LONG( 6 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            long firstBlock = block.getSingleValueBlock();
            long value = valueIsInlined( firstBlock ) ? (block.getSingleValueLong() >>> 1) : block.getValueBlocks()[1];
            return Property.longProperty( propertyKeyId, value );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Long.valueOf( getLongValue( block ) );
        }

        private long getLongValue( PropertyBlock block )
        {
            long firstBlock = block.getSingleValueBlock();
            return valueIsInlined( firstBlock ) ? (block.getSingleValueLong() >>> 1) :
                    block.getValueBlocks()[1];
        }

        private boolean valueIsInlined( long firstBlock )
        {
            // [][][][][   i,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
            return (firstBlock & 0x10000000L) > 0;
        }

        @Override
        public int calculateNumberOfBlocksUsed( long firstBlock )
        {
            return valueIsInlined( firstBlock ) ? 1 : 2;
        }
    },
    FLOAT( 7 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.floatProperty( propertyKeyId, Float.intBitsToFloat( block.getSingleValueInt() ) );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Float.valueOf( getValue( block.getSingleValueInt() ) );
        }

        private float getValue( int propBlock )
        {
            return Float.intBitsToFloat( propBlock );
        }
    },
    DOUBLE( 8 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.doubleProperty( propertyKeyId, Double.longBitsToDouble( block.getValueBlocks()[1] ) );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Double.valueOf( getValue( block.getValueBlocks()[1] ) );
        }

        private double getValue( long propBlock )
        {
            return Double.longBitsToDouble( propBlock );
        }

        @Override
        public int calculateNumberOfBlocksUsed( long firstBlock )
        {
            return 2;
        }
    },
    STRING( 9 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, final PropertyBlock block,
                                             final Supplier<PropertyStore> store )
        {
            return Property.lazyStringProperty(propertyKeyId, new Callable<String>()
            {
                @Override
                public String call() throws Exception
                {
                    return getValue( block, store.get() );
                }
            });
        }

        @Override
        public String getValue( PropertyBlock block, PropertyStore store )
        {
            if ( store == null )
            {
                return null;
            }
            return store.getStringFor( block );
        }

        @Override
        public byte[] readDynamicRecordHeader( byte[] recordBytes )
        {
            return EMPTY_BYTE_ARRAY;
        }
    },
    ARRAY( 10 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, final PropertyBlock block, final Supplier<PropertyStore> store )
        {
            return Property.lazyArrayProperty(propertyKeyId, new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    return getValue( block, store.get() );
                }
            });
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            if ( store == null )
            {
                return null;
            }
            return store.getArrayFor( block );
        }

        @Override
        public byte[] readDynamicRecordHeader( byte[] recordBytes )
        {
            byte itemType = recordBytes[0];
            if ( itemType == STRING.byteValue() )
            {
                return headOf( recordBytes, DynamicArrayStore.STRING_HEADER_SIZE );
            }
            else if ( itemType <= DOUBLE.byteValue() )
            {
                return headOf( recordBytes, DynamicArrayStore.NUMBER_HEADER_SIZE );
            }
            throw new IllegalArgumentException( "Unknown array type " + itemType );
        }

        private byte[] headOf( byte[] bytes, int length )
        {
            return Arrays.copyOf( bytes, length );
        }
    },
    SHORT_STRING( 11 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            return Property.stringProperty( propertyKeyId, LongerShortString.decode( block ) );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return LongerShortString.decode( block );
        }

        @Override
        public int calculateNumberOfBlocksUsed( long firstBlock )
        {
            return LongerShortString.calculateNumberOfBlocksUsed( firstBlock );
        }
    },
    SHORT_ARRAY( 12 )
    {
        @Override
        public DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store )
        {
            // TODO: Specialize per type
            return Property.property( propertyKeyId, ShortArray.decode(block) );
        }

        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return ShortArray.decode( block );
        }

        @Override
        public int calculateNumberOfBlocksUsed( long firstBlock )
        {
            return ShortArray.calculateNumberOfBlocksUsed( firstBlock );
        }
    };

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final int BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING = -1;

    private final int type;

    // TODO In wait of a better place
    private static int payloadSize = PropertyStore.DEFAULT_PAYLOAD_SIZE;

    PropertyType( int type )
    {
        this.type = type;
    }

    /**
     * Returns an int value representing the type.
     *
     * @return The int value for this property type
     */
    public int intValue()
    {
        return type;
    }

    /**
     * Returns a byte value representing the type. As long as there are
     * &lt 128 PropertyTypes, this should be equal to intValue(). When this
     * statement no longer holds, this method should be removed.
     *
     * @return The byte value for this property type
     */
    public byte byteValue()
    {
        return (byte) type;
    }

    public abstract Object getValue( PropertyBlock block, PropertyStore store );

    public abstract DefinedProperty readProperty( int propertyKeyId, PropertyBlock block, Supplier<PropertyStore> store );

    public static PropertyType getPropertyType( long propBlock, boolean nullOnIllegal )
    {
        // [][][][][    ,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        int type = (int)((propBlock&0x000000000F000000L)>>24);
        switch ( type )
        {
        case 1:
            return BOOL;
        case 2:
            return BYTE;
        case 3:
            return SHORT;
        case 4:
            return CHAR;
        case 5:
            return INT;
        case 6:
            return LONG;
        case 7:
            return FLOAT;
        case 8:
            return DOUBLE;
        case 9:
            return STRING;
        case 10:
            return ARRAY;
        case 11:
            return SHORT_STRING;
        case 12:
            return SHORT_ARRAY;
        default:
            if ( nullOnIllegal )
            {
                return null;
            }
            throw new InvalidRecordException( "Unknown property type for type "
                                              + type );
        }
    }

    // TODO In wait of a better place
    public static int getPayloadSize()
    {
        return payloadSize;
    }

    // TODO In wait of a better place
    public static int getPayloadSizeLongs()
    {
        return payloadSize >>> 3;
    }

    // TODO In wait of a better place
    public static void setPayloadSize( int newPayloadSize )
    {
        if ( newPayloadSize%8 != 0 )
        {
            throw new RuntimeException( "Payload must be divisible by 8" );
        }
        payloadSize = newPayloadSize;
    }

    public int calculateNumberOfBlocksUsed( long firstBlock )
    {
        return 1;
    }

    public byte[] readDynamicRecordHeader( byte[] recordBytes )
    {
        throw new UnsupportedOperationException();
    }
}
