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
package org.neo4j.io;

/**
 * A ByteUnit is a unit for a quantity of bytes.
 * <p/>
 * The unit knows how to convert between other units in its class, so you for instance can turn a number of KiBs into
 * an accurate quantity of bytes. Precision can be lost when converting smaller units into larger units, because of
 * integer division.
 *
 * These units all follow the EIC (International Electrotechnical Commission) standard, which uses a multiplier of
 * 1.024. This system is also known as the binary system, and has been accepted as part of the International System of
 * Quantities. It is therefor the recommended choice when communicating quantities of information, and the only one
 * available in this implementation.
 */
public enum ByteUnit
{
    /*
    XXX Future notes: This class can potentially replace some of the functionality in org.neo4j.helpers.Format.
     */

    Byte( 0, "B" ),
    KibiByte( 1, "KiB" ),
    MebiByte( 2, "MiB" ),
    GibiByte( 3, "GiB" ),
    TebiByte( 4, "TiB" ),
    PebiByte( 5, "PiB" ),
    ExbiByte( 6, "EiB" ),;

    private static final long EIC_MULTIPLIER = 1024;

    private final long factor;
    private final String shortName;

    ByteUnit( long power, String shortName )
    {
        this.factor = factorFromPower( power );
        this.shortName = shortName;
    }

    /**
     * Compute the increment factor from the given power.
     * <p/>
     * Giving zero always produces 1. Giving 1 will produce 1000 or 1024, for SI and EIC respectively, and so on.
     */
    private long factorFromPower( long power )
    {
        if ( power == 0 )
        {
            return 1;
        }
        long product = EIC_MULTIPLIER;
        for ( int i = 0; i < power - 1; i++ )
        {
            product = product * EIC_MULTIPLIER;
        }
        return product;
    }

    /**
     * Get the short or abbreviated name of this unit, e.g. KiB or MiB.
     *
     * @return The short unit name.
     */
    public String abbreviation()
    {
        return shortName;
    }

    /**
     * Convert the given value of this unit, to a value in the given unit.
     *
     * @param value The value to convert from this unit.
     * @param toUnit The unit of the resulting value.
     * @return The value in the given result unit.
     */
    public long convert( long value, ByteUnit toUnit )
    {
        return toBytes( value ) / toUnit.factor;
    }

    public long toBytes( long value )
    {
        return factor * value;
    }

    public long toKibiBytes( long value )
    {
        return convert( value, KibiByte );
    }

    public long toMebiBytes( long value )
    {
        return convert( value, MebiByte );
    }

    public long toGibiBytes( long value )
    {
        return convert( value, GibiByte );
    }

    public long toTebiBytes( long value )
    {
        return convert( value, TebiByte );
    }

    public long toPebiBytes( long value )
    {
        return convert( value, PebiByte );
    }

    public long toExbiBytes( long value )
    {
        return convert( value, ExbiByte );
    }

    public static long bytes( long bytes )
    {
        return bytes;
    }

    public static long kibiBytes( long kibibytes )
    {
        return KibiByte.toBytes( kibibytes );
    }

    public static long mebiBytes( long mebibytes )
    {
        return MebiByte.toBytes( mebibytes );
    }

    public static long gibiBytes( long gibibytes )
    {
        return GibiByte.toBytes( gibibytes );
    }

    public static long tebiBytes( long tebibytes )
    {
        return TebiByte.toBytes( tebibytes );
    }

    public static long pebiBytes( long pebibytes )
    {
        return PebiByte.toBytes( pebibytes );
    }

    public static long exbiBytes( long exbibytes )
    {
        return ExbiByte.toBytes( exbibytes );
    }
}
