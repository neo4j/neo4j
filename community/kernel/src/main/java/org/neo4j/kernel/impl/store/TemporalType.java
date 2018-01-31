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

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;

/**
 * For the PropertyStore format, check {@link PropertyStore}.
 * For the array format, check {@link DynamicArrayStore}.
 */
public enum TemporalType
{
    TEMPORAL_INVALID( 0, "Invalid" )
            {
                @Override
                public Value decodeForTemporal( long[] valueBlocks, int offset )
                {
                    throw new UnsupportedOperationException( "Cannot decode invalid temporal" );
                }

                @Override
                public int calculateNumberOfBlocksUsedForTemporal( long firstBlock )
                {
                    return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
                }

                @Override
                public ArrayValue decodeArray( TemporalHeader header, byte[] data )
                {
                    throw new UnsupportedOperationException( "Cannot decode invalid temporal array" );
                }
            },
    TEMPORAL_DATE( 1, "Date" )
            {
                @Override
                public Value decodeForTemporal( long[] valueBlocks, int offset )
                {
                    long epochDay = valueIsInlined( valueBlocks[0] ) ? valueBlocks[offset] >>> 33 : valueBlocks[1 + offset];
                    return DateValue.epochDate( epochDay );
                }

                @Override
                public int calculateNumberOfBlocksUsedForTemporal( long firstBlock )
                {
                    return valueIsInlined( firstBlock ) ? 1 : 2;
                }

                @Override
                public ArrayValue decodeArray( TemporalHeader header, byte[] data )
                {
                    throw new UnsupportedOperationException( "Add support for arrays" );
                }

                private boolean valueIsInlined( long firstBlock )
                {
                    // [][][][i][ssss,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
                    return (firstBlock & 0x100000000L) > 0;
                }
            },
    TEMPORAL_LOCAL_TIME( 2, "LocalTime" )
            {
                @Override
                public Value decodeForTemporal( long[] valueBlocks, int offset )
                {
                    long nanoOfDay = valueIsInlined( valueBlocks[0] ) ? valueBlocks[offset] >>> 33 : valueBlocks[1 + offset];
                    return LocalTimeValue.localTime( nanoOfDay );
                }

                @Override
                public int calculateNumberOfBlocksUsedForTemporal( long firstBlock )
                {
                    return valueIsInlined( firstBlock ) ? 1 : 2;
                }

                @Override
                public ArrayValue decodeArray( TemporalHeader header, byte[] data )
                {
                    throw new UnsupportedOperationException( "Add support for arrays" );
                }

                private boolean valueIsInlined( long firstBlock )
                {
                    // [][][][i][ssss,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
                    return (firstBlock & 0x100000000L) > 0;
                }
            },
    TEMPORAL_LOCAL_DATE_TIME( 3, "LocalDateTime" )
            {
                @Override
                public Value decodeForTemporal( long[] valueBlocks, int offset )
                {
                    long nanoOfSecond = valueBlocks[offset] >>> 32;
                    long epochSecond = valueBlocks[1 + offset];
                    return LocalDateTimeValue.localDateTime( epochSecond, nanoOfSecond );
                }

                @Override
                public int calculateNumberOfBlocksUsedForTemporal( long firstBlock )
                {
                    return 2;
                }

                @Override
                public ArrayValue decodeArray( TemporalHeader header, byte[] data )
                {
                    throw new UnsupportedOperationException( "Add support for arrays" );
                }
            },
    TEMPORAL_TIME( 4, "Time" )
            {
                @Override
                public Value decodeForTemporal( long[] valueBlocks, int offset )
                {
                    int secondOffset = (int) (valueBlocks[offset] >>> 32);
                    long nanoOfDay = valueBlocks[1 + offset];
                    return TimeValue.time( nanoOfDay, ZoneOffset.ofTotalSeconds( secondOffset ) );
                }

                @Override
                public int calculateNumberOfBlocksUsedForTemporal( long firstBlock )
                {
                    return 2;
                }

                @Override
                public ArrayValue decodeArray( TemporalHeader header, byte[] data )
                {
                    throw new UnsupportedOperationException( "Add support for arrays" );
                }
            },
    TEMPORAL_DATE_TIME( 5, "DateTime" )
            {
                @Override
                public Value decodeForTemporal( long[] valueBlocks, int offset )
                {
                    if ( storingZoneOffset( valueBlocks[0] ) )
                    {
                        int nanoOfSecond = (int) (valueBlocks[offset] >>> 33);
                        long epochSecond = valueBlocks[1 + offset];
                        int secondOffset = (int) valueBlocks[2 + offset];
                        return DateTimeValue.datetime( epochSecond, nanoOfSecond, ZoneOffset.ofTotalSeconds( secondOffset ) );
                    }
                    else
                    {
                        throw new UnsupportedOperationException( "..." );
                    }
                }

                @Override
                public int calculateNumberOfBlocksUsedForTemporal( long firstBlock )
                {
                    if ( storingZoneOffset( firstBlock ) )
                    {
                        return 3;
                    }
                    else
                    {
                        // TODO proper number
                        return 3;
                    }
                }

                @Override
                public ArrayValue decodeArray( TemporalHeader header, byte[] data )
                {
                    throw new UnsupportedOperationException( "Add support for arrays" );
                }

                private boolean storingZoneOffset( long firstBlock )
                {
                    // [][][][i][ssss,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
                    return (firstBlock & 0x100000000L) > 0;
                }
            },
    TEMPORAL_DURATION( 6, "Duration" )
        {
            @Override
            public Value decodeForTemporal( long[] valueBlocks, int offset )
            {
                int nanos = (int) (valueBlocks[offset] >>> 32);
                long months = valueBlocks[1 + offset];
                long days = valueBlocks[2 + offset];
                long seconds = valueBlocks[3 + offset];
                return DurationValue.duration( months, days, seconds, nanos );
            }

            @Override
            public int calculateNumberOfBlocksUsedForTemporal( long firstBlock )
            {
                return 4;
            }

            @Override
            public ArrayValue decodeArray( TemporalHeader header, byte[] data )
            {
                throw new UnsupportedOperationException( "Add support for arrays" );
            }
        };

    /**
     * Handler for header information for Temporal objects and arrays of Temporal objects
     */
    public static class TemporalHeader
    {
        private final int temporalType;

        private TemporalHeader( int temporalType )
        {
            this.temporalType = temporalType;
        }

        private void writeArrayHeaderTo( byte[] bytes )
        {
            throw new UnsupportedOperationException( "Add support for arrays" );
        }

        static TemporalHeader fromArrayHeaderBytes( byte[] header )
        {
            throw new UnsupportedOperationException( "Add support for arrays" );
        }

        public static TemporalHeader fromArrayHeaderByteBuffer( ByteBuffer buffer )
        {
            throw new UnsupportedOperationException( "Add support for arrays" );
        }
    }

    private static final TemporalType[] TYPES = TemporalType.values();
    private static final Map<String,TemporalType> all = new HashMap<>( TYPES.length );

    static
    {
        for ( TemporalType temporalType : TYPES )
        {
            all.put( temporalType.name, temporalType );
        }
    }

    private static final long TEMPORAL_TYPE_MASK = 0x00000000F0000000L;

    private static int getTemporalType( long firstBlock )
    {
        return (int) ((firstBlock & TEMPORAL_TYPE_MASK) >> 28);
    }

    public static int calculateNumberOfBlocksUsed( long firstBlock )
    {
        TemporalType geometryType = find( getTemporalType( firstBlock ) );
        return geometryType.calculateNumberOfBlocksUsedForTemporal( firstBlock );
    }

    private static TemporalType find( int temporalType )
    {
        if ( temporalType < TYPES.length )
        {
            return TYPES[temporalType];
        }
        else
        {
            // Kernel code requires no exceptions in deeper PropertyChain processing of corrupt/invalid data
            return TEMPORAL_INVALID;
        }
    }

    public static TemporalType find( String name )
    {
        TemporalType table = all.get( name );
        if ( table != null )
        {
            return table;
        }
        else
        {
            throw new IllegalArgumentException( "No known Temporal Type: " + name );
        }
    }

    public static Value decode( PropertyBlock block )
    {
        return decode( block.getValueBlocks(), 0 );
    }

    public static Value decode( long[] valueBlocks, int offset )
    {
        long firstBlock = valueBlocks[offset];
        int temporalType = getTemporalType( firstBlock );
        return find( temporalType ).decodeForTemporal( valueBlocks, offset );
    }

    public static long[] encodeDate( int keyId, long epochDay )
    {
        return encodeLong( keyId, epochDay, TemporalType.TEMPORAL_DATE.temporalType );
    }

    public static long[] encodeLocalTime( int keyId, long nanoOfDay )
    {
        return encodeLong( keyId, nanoOfDay, TemporalType.TEMPORAL_LOCAL_TIME.temporalType );
    }

    private static long[] encodeLong( int keyId, long val, int temporalType )
    {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = temporalType << (idBits + 4);

        long[] data;
        if ( ShortArray.LONG.getRequiredBits( val ) <= 64 - 33 )
        {   // We only need one block for this value
            data = new long[1];
            data[0] = keyAndType | temporalTypeBits | (1L << 32) | (val << 33);
        }
        else
        {   // We need two blocks for this value
            data = new long[2];
            data[0] = keyAndType | temporalTypeBits;
            data[1] = val;
        }

        return data;
    }

    public static long[] encodeLocalDateTime( int keyId, long epochSecond, long nanoOfSecond )
    {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_LOCAL_DATE_TIME.temporalType << (idBits + 4);

        long[] data = new long[2];
        // nanoOfSecond will never require more than 30 bits
        data[0] = keyAndType | temporalTypeBits | (nanoOfSecond << 32);
        data[1] = epochSecond;

        return data;
    }

    public static long[] encodeDateTime( int keyId, long epochSecond, long nanoOfSecond, String zoneId )
    {
        throw new UnsupportedOperationException( "Cannot yet store DateTime with ZoneID" );
    }

    public static long[] encodeDateTime( int keyId, long epochSecond, long nanoOfSecond, int secondOffset )
    {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_DATE_TIME.temporalType << (idBits + 4);

        long[] data = new long[3];
        // nanoOfSecond will never require more than 30 bits
        data[0] = keyAndType | temporalTypeBits | (1L << 32) | (nanoOfSecond << 33);
        data[1] = epochSecond;
        data[2] = secondOffset;

        return data;
    }

    public static long[] encodeTime( int keyId, long nanoOfDay, int secondOffset )
    {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_TIME.temporalType << (idBits + 4);

        long[] data = new long[2];
        // Offset are always in the range +-18:00:00, so secondOffset will never require more than 17 bits
        data[0] = keyAndType | temporalTypeBits | ((long) secondOffset << 32);
        data[1] = nanoOfDay;

        return data;
    }

    public static long[] encodeDuration( int keyId, long months, long days, long seconds, int nanos)
    {
        int idBits = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;

        long keyAndType = keyId | (((long) (PropertyType.TEMPORAL.intValue()) << idBits));
        long temporalTypeBits = TemporalType.TEMPORAL_DURATION.temporalType << (idBits + 4);

        long[] data = new long[4];
        data[0] = keyAndType | temporalTypeBits | ((long) nanos << 32);
        data[1] = months;
        data[2] = days;
        data[3] = seconds;

        return data;
    }

    // TODO encode Array methods.

    public static ArrayValue decodeTemporalArray( TemporalHeader header, byte[] data )
    {
        return find( header.temporalType ).decodeArray( header, data );
    }

    private final int temporalType;
    private final String name;

    TemporalType( int temporalType, String name )
    {
        this.temporalType = temporalType;
        this.name = name;
    }

    public abstract Value decodeForTemporal( long[] valueBlocks, int offset );

    public abstract int calculateNumberOfBlocksUsedForTemporal( long firstBlock );

    public abstract ArrayValue decodeArray( TemporalHeader header, byte[] data );

    public int getTemporalType()
    {
        return temporalType;
    }

    public String getName()
    {
        return name;
    }
}
