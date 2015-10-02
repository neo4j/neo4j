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
package org.neo4j.io;

/**
 * A ByteUnit is a unit for a quantity of bytes.
 *
 * The unit knows how to convert between other units in its class, so you for instance can turn a number of KiBs into
 * an accurate quantity of bytes. Some loss can occur when quantities are converted between units, due to integer
 * rounding to whole units.
 *
 * The larger units can be in one of two systems of units. See the {@link ByteUnitSystem} for details.
 */
public enum ByteUnit
{
    /*
    XXX Future notes: This class can potentially replace some of the functionality in org.neo4j.helpers.Format.
     */

    Byte( 0, ByteUnitSystem.SI, "B" ),
    KiloByte( 1, ByteUnitSystem.SI, "kB" ),
    KibiByte( 1, ByteUnitSystem.EIC, "KiB" ),
    MegaByte( 2, ByteUnitSystem.SI, "MB" ),
    MebiByte( 2, ByteUnitSystem.EIC, "MiB" ),
    GigaByte( 3, ByteUnitSystem.SI, "GB" ),
    GibiByte( 3, ByteUnitSystem.EIC, "GiB" ),
    TeraByte( 4, ByteUnitSystem.SI, "TB" ),
    TebiByte( 4, ByteUnitSystem.EIC, "TiB" ),
    PetaByte( 5, ByteUnitSystem.SI, "PB" ),
    PebiByte( 5, ByteUnitSystem.EIC, "PiB" ),
    ExaByte( 6, ByteUnitSystem.SI, "EB" ),
    ExbiByte( 6, ByteUnitSystem.EIC, "EiB" ),
    ;

    private final long factor;
    private final ByteUnitSystem unitSystem;
    private final String shortName;

    ByteUnit( long power, ByteUnitSystem unitSystem, String shortName )
    {
        this.factor = unitSystem.factorFromPower( power );
        this.unitSystem = unitSystem;
        this.shortName = shortName;
    }

    /**
     * Get the {@link ByteUnitSystem} of this unit, which determines its conversion factor.
     * @return The ByteUnitSystem used.
     */
    public ByteUnitSystem getUnitSystem()
    {
        return unitSystem;
    }

    /**
     * Get the short or abbreviated name of this unit, e.g. kB or MiB.
     * @return The short unit name.
     */
    public String getShortName()
    {
        return shortName;
    }

    /**
     * Convert the given value of this unit, to a value in the given unit.
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

    public long toKiloBytes( long value )
    {
        return convert( value, KiloByte );
    }

    public long toKibiBytes( long value )
    {
        return convert( value, KibiByte );
    }

    public long toMegaBytes( long value )
    {
        return convert( value, MegaByte );
    }

    public long toMebiBytes( long value )
    {
        return convert( value, MebiByte );
    }

    public long toGigaBytes( long value )
    {
        return convert( value, GigaByte );
    }

    public long toGibiBytes( long value )
    {
        return convert( value, GibiByte );
    }

    public long toTeraBytes( long value )
    {
        return convert( value, TeraByte );
    }

    public long toTebiBytes( long value )
    {
        return convert( value, TebiByte );
    }

    public long toPetaBytes( long value )
    {
        return convert( value, PetaByte );
    }

    public long toPebiBytes( long value )
    {
        return convert( value, PebiByte );
    }

    public long toExaBytes( long value )
    {
        return convert( value, ExaByte );
    }

    public long toExbiBytes( long value )
    {
        return convert( value, ExbiByte );
    }

    public static long bytes( long bytes )
    {
        return bytes;
    }

    public static long kiloBytes( long kilobytes )
    {
        return KiloByte.toBytes( kilobytes );
    }

    public static long kibiBytes( long kibibytes )
    {
        return KibiByte.toBytes( kibibytes );
    }

    public static long megaBytes( long megabytes )
    {
        return MegaByte.toBytes( megabytes );
    }

    public static long mebiBytes( long mebibytes )
    {
        return MebiByte.toBytes( mebibytes );
    }

    public static long gigaBytes( long gigabytes )
    {
        return GigaByte.toBytes( gigabytes );
    }

    public static long gibiBytes( long gibibytes )
    {
        return GibiByte.toBytes( gibibytes );
    }

    public static long teraBytes( long terabytes )
    {
        return TeraByte.toBytes( terabytes );
    }

    public static long tebiBytes( long tebibytes )
    {
        return TebiByte.toBytes( tebibytes );
    }

    public static long petaBytes( long petabytes )
    {
        return PetaByte.toBytes( petabytes );
    }

    public static long pebiBytes( long pebibytes )
    {
        return PebiByte.toBytes( pebibytes );
    }

    public static long exaBytes( long exabytes )
    {
        return ExaByte.toBytes( exabytes );
    }

    public static long exbiBytes( long exbibytes )
    {
        return ExbiByte.toBytes( exbibytes );
    }
}
