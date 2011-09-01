/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

/**
 * Defines valid property types.
 */
public enum PropertyType
{
    BOOL( 1, 1 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return getValue( block.getValueBlocks()[0] );
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
                    getValue( block.getValueBlocks()[0] ).booleanValue() );
        }
    },
    BYTE( 2, 1 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Byte.valueOf( (byte) block.getValueBlocks()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forByte( block.getKeyIndexId(), propertyId,
                    (byte) block.getValueBlocks()[0] );
        }
    },
    SHORT( 3, 1 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Short.valueOf( (short) block.getValueBlocks()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forShort( block.getKeyIndexId(), propertyId,
                    (short) block.getValueBlocks()[0] );
        }
    },
    CHAR( 4, 1 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Character.valueOf( (char) block.getValueBlocks()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forChar( block.getKeyIndexId(), propertyId,
                    (char) block.getValueBlocks()[0] );
        }
    },
    INT( 5, 1 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Integer.valueOf( (int) block.getValueBlocks()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            // TODO : The masking off of bits should not happen here
            return PropertyDatas.forInt( block.getKeyIndexId(), propertyId,
                    (int) block.getValueBlocks()[0] );
        }
    },
    LONG( 6, 2 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Long.valueOf( block.getValueBlocks()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forLong( block.getKeyIndexId(), propertyId,
                    block.getValueBlocks()[1] );
        }
    },
    FLOAT( 7, 1 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Float.valueOf( getValue( block.getValueBlocks()[0] ) );
        }

        private float getValue( long propBlock )
        {
            return Float.intBitsToFloat( (int) propBlock );
        }

        @Override
        public PropertyData newPropertyData( PropertyBlock block,
                long propertyId, Object extractedValue )
        {
            return PropertyDatas.forFloat( block.getKeyIndexId(), propertyId,
                    getValue( block.getValueBlocks()[0] ) );
        }
    },
    DOUBLE( 8, 2 )
    {
        @Override
        public Object getValue( PropertyBlock block, PropertyStore store )
        {
            return Double.valueOf( Double.longBitsToDouble( block.getValueBlocks()[1] ) );
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
    },
    STRING( 9, 1 )
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
    },
    ARRAY( 10, 1 )
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
    },
    SHORT_STRING( 11, 4 )
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
    },
    SHORT_ARRAY( 12, 4 )
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
    };

    private final int type;

    private final int sizeOfBlockInLongs;

    // TODO In wait of a better place
    private static int payloadSize = PropertyStore.DEFAULT_PAYLOAD_SIZE;

    PropertyType( int type, int sizeOfBlockInLongs )
    {
        this.type = type;
        this.sizeOfBlockInLongs = sizeOfBlockInLongs;
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

    public int getSizeInLongs()
    {
        return sizeOfBlockInLongs;
    }

    public static PropertyType getPropertyType( long propBlock, boolean nullOnIllegal )
    {
        // [kkkk,kkkk][kkkk,kkkk][kkkk,kkkk][tttt,    ][][][][]
        int type = (int)((propBlock&0x000000F000000000L)>>36);
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
}
