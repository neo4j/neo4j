/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import org.neo4j.kernel.impl.api.LabelAsProperty;
import org.neo4j.kernel.impl.api.LabelAsPropertyData;

/**
 * Defines valid property types.
 */
public enum PropertyType
{
    BOOL( 1 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return getValue( block.getSingleValueLong() );
        }

        private Boolean getValue( long propBlock )
        {
            return ( propBlock & 0x1 ) == 1 ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forBoolean( block.getKeyIndexId(), propertyId,
                    getValue( block.getSingleValueLong() ).booleanValue() );
        }
    },
    BYTE( 2 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Byte.valueOf( (byte) block.getSingleValueByte() );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forByte( block.getKeyIndexId(), propertyId,
                    block.getSingleValueByte() );
        }
    },
    SHORT( 3 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Short.valueOf( block.getSingleValueShort() );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forShort( block.getKeyIndexId(), propertyId,
                    block.getSingleValueShort() );
        }
    },
    CHAR( 4 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Character.valueOf( (char) block.getSingleValueShort() );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forChar( block.getKeyIndexId(), propertyId,
                    (char) block.getSingleValueShort() );
        }
    },
    INT( 5 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Integer.valueOf( block.getSingleValueInt() );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forInt( block.getKeyIndexId(), propertyId,
                    block.getSingleValueInt() );
        }
    },
    LONG( 6 )
    {
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

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forLong( block.getKeyIndexId(), propertyId, getLongValue( block ) );
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
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Float.valueOf( getValue( block.getSingleValueInt() ) );
        }

        private float getValue( int propBlock )
        {
            return Float.intBitsToFloat( (int) propBlock );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forFloat( block.getKeyIndexId(), propertyId,
                    getValue( block.getSingleValueInt() ) );
        }
    },
    DOUBLE( 8 )
    {
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
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forDouble( block.getKeyIndexId(), propertyId,
                    getValue( block.getValueBlocks()[1] ) );
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
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            if ( store == null ) return null;
            return store.getStringFor( block );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( block.getKeyIndexId(),
                    propertyId, extractedValue );
        }
        
        @Override
        byte[] readDynamicRecordHeader( byte[] recordBytes )
        {
            return new byte[0];
        }
    },
    ARRAY( 10 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            if ( store == null ) return null;
            return store.getArrayFor( block );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( block.getKeyIndexId(),
                    propertyId, extractedValue );
        }
        
        @Override
        byte[] readDynamicRecordHeader( byte[] recordBytes )
        {
            byte itemType = recordBytes[0];
            if ( itemType == STRING.byteValue() )
                return headOf( recordBytes, DynamicArrayStore.STRING_HEADER_SIZE );
            else if ( itemType <= DOUBLE.byteValue() )
                return headOf( recordBytes, DynamicArrayStore.NUMBER_HEADER_SIZE );
            throw new IllegalArgumentException( "Unknown array type " + itemType );
        }

        private byte[] headOf( byte[] bytes, int length )
        {
            byte[] head = new byte[length];
            System.arraycopy( bytes, 0, head, 0, length );
            return head;
        }
    },
    SHORT_STRING( 11 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return LongerShortString.decode( block );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( block.getKeyIndexId(),
                    propertyId, getValue( block, null ) );
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
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return ShortArray.decode( block );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( block.getKeyIndexId(),
                    propertyId, getValue( block, null ) );
        }
        
        @Override
        public int calculateNumberOfBlocksUsed( long firstBlock )
        {
            return ShortArray.calculateNumberOfBlocksUsed( firstBlock );
        }
    },
    LABEL( 13 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return new LabelAsProperty( block.getValueBlocks()[1] );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block, long propertyId, Object extractedValue )
        {
            return new LabelAsPropertyData( propertyId, block.getKeyIndexId(), getValue( block, null ) );
        }

        @Override
        public int calculateNumberOfBlocksUsed( long firstBlock )
        {
            return 2;
        }
    };

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

    public abstract PropertyData newPropertyData( PropertyBlock block,
            long propertyId, Object extractedValue );

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
        case 13:
            return LABEL;
        default: if (nullOnIllegal) return null;
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
    
    byte[] readDynamicRecordHeader( byte[] recordBytes )
    {
        throw new UnsupportedOperationException();
    }
}
