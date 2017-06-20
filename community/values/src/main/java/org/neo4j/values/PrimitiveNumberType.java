package org.neo4j.values;

public enum PrimitiveNumberType
{
    BYTE( (byte) 0 )
    {
        @Override
        public NumberValue valueFromRawBits( long rawBits )
        {
            return (NumberValue) Values.byteValue( (byte) rawBits );
        }
    },
    SHORT( (byte) 1 )
    {
        @Override
        public NumberValue valueFromRawBits( long rawBits )
        {
            return (NumberValue) Values.shortValue( (short) rawBits );
        }
    },
    INT( (byte) 2 )
    {
        @Override
        public NumberValue valueFromRawBits( long rawBits )
        {
            return (NumberValue) Values.intValue( (int) rawBits );
        }
    },
    LONG( (byte) 3 )
    {
        @Override
        public NumberValue valueFromRawBits( long rawBits )
        {
            return (NumberValue) Values.longValue( rawBits );
        }
    },
    FLOAT( (byte) 4 )
    {
        @Override
        public NumberValue valueFromRawBits( long rawBits )
        {
            return (NumberValue) Values.floatValue( Float.intBitsToFloat( (int) rawBits ) );
        }
    },
    DOUBLE( (byte) 5 )
    {
        @Override
        public NumberValue valueFromRawBits( long rawBits )
        {
            return (NumberValue) Values.doubleValue( Double.longBitsToDouble( rawBits ) );
        }
    };

    private byte byteRepresentation;

    PrimitiveNumberType( byte byteRepresentation )
    {
        this.byteRepresentation = byteRepresentation;
    }

    public byte byteRepresentation()
    {
        return byteRepresentation;
    }

    public abstract NumberValue valueFromRawBits( long rawBits );

    public static PrimitiveNumberType from( byte byteRepresentation )
    {
        switch ( byteRepresentation )
        {
        case 0:
            return BYTE;
        case 1:
            return SHORT;
        case 2:
            return INT;
        case 3:
            return LONG;
        case 4:
            return FLOAT;
        case 5:
            return DOUBLE;
        default:
            throw new IllegalArgumentException( "Expected byteRepresentation of type 0-5 (inclusive), was " + byteRepresentation );
        }
    }
}
