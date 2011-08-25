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
    BOOL( 0 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return getValue( record.getPropBlock()[0] );
        }

        private Boolean getValue( long propBlock )
        {
            return ( propBlock & 0x1 ) == 1 ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forBoolean( record.getKeyIndexId(), record.getId(),
                    getValue( record.getPropBlock()[0] ).booleanValue() );
        }
    },
    BYTE( 1 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Byte.valueOf( (byte) record.getPropBlock()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forByte( record.getKeyIndexId(), record.getId(), (byte) record.getPropBlock()[0] );
        }
    },
    SHORT( 2 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Short.valueOf( (short) record.getPropBlock()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forShort( record.getKeyIndexId(), record.getId(), (short) record.getPropBlock()[0] );
        }
    },
    CHAR( 3 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Character.valueOf( (char) record.getPropBlock()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forChar( record.getKeyIndexId(), record.getId(), (char) record.getPropBlock()[0] );
        }
    },
    INT( 4 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Integer.valueOf( (int) record.getPropBlock()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forInt( record.getKeyIndexId(), record.getId(), (int) record.getPropBlock()[0] );
        }
    },
    LONG( 5 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Long.valueOf( record.getPropBlock()[0] );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forLong( record.getKeyIndexId(), record.getId(), record.getPropBlock()[1] );
        }
    },
    FLOAT( 6 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Float.valueOf( getValue( record.getPropBlock()[0] ) );
        }

        private float getValue( long propBlock )
        {
            return Float.intBitsToFloat( (int) propBlock );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forFloat( record.getKeyIndexId(), record.getId(), getValue( record.getPropBlock()[0] ) );
        }
    },
    DOUBLE( 7 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return Double.valueOf( Double.longBitsToDouble( record.getPropBlock()[1] ) );
        }

        private double getValue( long propBlock )
        {
            return Double.longBitsToDouble( propBlock );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forDouble( record.getKeyIndexId(),
                    record.getId(), getValue( record.getPropBlock()[1] ) );
        }
    },
    STRING( 8 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            if ( store == null ) return null;
            return store.getStringFor( record );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( record.getKeyIndexId(), record.getId(), extractedValue );
        }
    },
    ARRAY( 9 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            if ( store == null ) return null;
            return store.getArrayFor( record );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( record.getKeyIndexId(), record.getId(), extractedValue );
        }
    },
    SHORT_STRING( 10 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return LongerShortString.decode( record );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( record.getKeyIndexId(), record.getId(), getValue( record, null ) );
        }
    },
    SHORT_ARRAY( 11 )
    {
        @Override
        public Object getValue( PropertyRecord record, PropertyStore store )
        {
            return ShortArray.decode( record );
        }

        @Override
        public PropertyData newPropertyData( PropertyRecord record, Object extractedValue )
        {
            return PropertyDatas.forStringOrArray( record.getKeyIndexId(), record.getId(),
                    getValue( record, null ) );
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

    public abstract Object getValue( PropertyRecord record, PropertyStore store );

    public abstract PropertyData newPropertyData( PropertyRecord record, Object extractedValue );

    public static PropertyType getPropertyType( long propBlock, boolean nullOnIllegal )
    {
        // [kkkk,kkkk][kkkk,kkkk][kkkk,kkkk][tttt,    ][][][][]
        int type = (int)((propBlock&0x000000F000000000L)>>36);
        switch ( type )
        {
        case 0: return BOOL;
        case 1: return BYTE;
        case 2: return SHORT;
        case 3: return CHAR;
        case 4: return INT;
        case 5: return LONG;
        case 6: return FLOAT;
        case 7: return DOUBLE;
        case 8: return STRING;
        case 9: return ARRAY;
        case 10: return SHORT_STRING;
        case 11: return SHORT_ARRAY;
        default: throw new InvalidRecordException( "Unknown property type for type " + type );
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
