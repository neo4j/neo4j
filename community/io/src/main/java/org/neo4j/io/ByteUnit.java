/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * A ByteUnit is a unit for a quantity of bytes.
 * <p>
 * The unit knows how to convert between other units in its class, so you for instance can turn a number of KiBs into
 * an accurate quantity of bytes. Precision can be lost when converting smaller units into larger units, because of
 * integer division.
 * <p>
 * These units all follow the EIC (International Electrotechnical Commission) standard, which uses a multiplier of
 * 1.024. This system is also known as the binary system, and has been accepted as part of the International System of
 * Quantities. It is therefor the recommended choice when communicating quantities of information, and the only one
 * available in this implementation.
 */
public enum ByteUnit
{
    /*
    XXX Future notes: This class can potentially replace some of the functionality in org.neo4j.helpers.internal.Format.
     */

    Byte( 0, "B" ),
    KibiByte( 1, "KiB", "KB", "K", "kB", "kb", "k" ),
    MebiByte( 2, "MiB", "MB", "M", "mB", "mb", "m" ),
    GibiByte( 3, "GiB", "GB", "G", "gB", "gb", "g" ),
    TebiByte( 4, "TiB", "TB" ),
    PebiByte( 5, "PiB", "PB" ),
    ExbiByte( 6, "EiB", "EB" );

    public static final long ONE_KIBI_BYTE = ByteUnit.KibiByte.toBytes( 1 );
    public static final long ONE_MEBI_BYTE = ByteUnit.MebiByte.toBytes( 1 );
    public static final long ONE_GIBI_BYTE = ByteUnit.GibiByte.toBytes( 1 );
    public static final long ONE_TEBI_BYTE = ByteUnit.TebiByte.toBytes( 1 );
    public static final String VALID_MULTIPLIERS = Arrays.stream( ByteUnit.values() ).flatMap( unit -> Arrays.stream( unit.names ) )
            .collect( Collectors.joining("`, `", "`", "`" ));

    private static final long EIC_MULTIPLIER = 1024;

    private final long factor;
    private final String shortName;
    private final String[] names;

    ByteUnit( long power, String... names )
    {
        this.factor = factorFromPower( power );
        this.shortName = names[0];
        this.names = names;
    }

    /**
     * Compute the increment factor from the given power.
     * <p>
     * Giving zero always produces 1. Giving 1 will produce 1024, and so on.
     */
    private static long factorFromPower( long power )
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

    private double toBytesFromDecimal( double value )
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

    private static String bytesToString( long bytes, Locale locale, boolean allowScientificNotation )
    {
        String format = allowScientificNotation ? "%.4g%s" : "%.2f%s";
        if ( bytes >= ONE_TEBI_BYTE )
        {
            return format( locale, format, bytes / (double) ONE_TEBI_BYTE, TebiByte.shortName );
        }
        else if ( bytes >= ONE_GIBI_BYTE )
        {
            return format( locale, format, bytes / (double) ONE_GIBI_BYTE, GibiByte.shortName );
        }
        else if ( bytes >= ONE_MEBI_BYTE )
        {
            return format( locale, format, bytes / (double) ONE_MEBI_BYTE, MebiByte.shortName );
        }
        else if ( bytes >= ONE_KIBI_BYTE )
        {
            return format( locale, format, bytes / (double) ONE_KIBI_BYTE, KibiByte.shortName );
        }
        else
        {
            return bytes + Byte.shortName;
        }
    }

    public static String bytesToString( long bytes )
    {
        return bytesToString( bytes, Locale.ROOT, true );
    }

    /**
     * The parse method doesn't support values with scientific notation. This bytes to string method builds a String
     * representation that always uses decimal floating-point.
     * This should be used when the String representation might get parsed e.g values used in documentation.
     */
    public static String bytesToStringWithoutScientificNotation( long bytes )
    {
        return bytesToString( bytes, Locale.ENGLISH, false );
    }

    public static long parse( String text )
    {
        String trimmedText = text.trim();
        int len = trimmedText.length();
        int unitStart;
        int unitCharacters = 0;
        int i = 0;

        // Parse digits.
        NumberFormat numberInstance = NumberFormat.getNumberInstance( Locale.ENGLISH );
        ParsePosition pos = new ParsePosition( i );
        Number number = numberInstance.parse( trimmedText, pos );

        // The index will be unchanged if an error occurred or there were no digits.
        int indexAfterDigits = pos.getIndex();
        if ( indexAfterDigits == i )
        {
            throw invalidFormat( text );
        }
        i = indexAfterDigits;

        checkValueInRange( number.doubleValue(), text );

        // Skip whitespace between digits and unit.
        while ( i < len && Character.isWhitespace( trimmedText.charAt( i ) ) )
        {
            i++;
        }

        // Parse the unit.
        unitStart = i;
        while ( i < len && !Character.isWhitespace( trimmedText.charAt( i ) ) )
        {
            i++;
            unitCharacters++;
        }

        if ( unitCharacters == 0 )
        {
            return number.longValue();
        }

        ByteUnit unit = listUnits().get( trimmedText.substring( unitStart, unitStart + unitCharacters ) );
        if ( unit == null )
        {
            throw invalidFormat( text );
        }

        double inBytes = unit.toBytesFromDecimal( number.doubleValue() );
        checkValueInRange( inBytes, text );
        return (long)inBytes;
    }

    private static IllegalArgumentException invalidFormat( String text )
    {
        return new IllegalArgumentException(
                format( "'%s' is not a valid size, must be e.g. 10, 5K, 1M, 11G (valid multipliers are %s)", text, VALID_MULTIPLIERS ) );
    }

    private static void checkValueInRange( double value, String originalValue )
    {
        if ( value < 0 || value > Long.MAX_VALUE )
        {
            throw new IllegalArgumentException( "'" + originalValue + "' is not a valid size. Value should be between 0 and " +
                                                bytesToStringWithoutScientificNotation( Long.MAX_VALUE ) );
        }
    }

    private static Map<String,ByteUnit> listUnits()
    {
        Map<String,ByteUnit> units = new HashMap<>();
        for ( ByteUnit unit : values() )
        {
            for ( String name : unit.names )
            {
                units.put( name, unit );
            }
        }
        return units;
    }
}
