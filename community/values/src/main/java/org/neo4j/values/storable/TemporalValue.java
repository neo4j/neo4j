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
package org.neo4j.values.storable;

import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.time.temporal.ValueRange;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.neo4j.values.storable.NumberType.NO_NUMBER;

public abstract class TemporalValue<T extends Temporal, V extends TemporalValue<T,V>>
        extends ScalarValue implements Temporal
{
    TemporalValue()
    {
        // subclasses are confined to this package,
        // but type-checking is valuable to be able to do outside
        // (therefore the type itself is public)
    }

    private static final String DEFAULT_WHEN = "statement";

    public abstract TemporalValue add( DurationValue duration );

    public abstract TemporalValue sub( DurationValue duration );

    abstract T temporal();

    abstract V replacement( T temporal );

    @Override
    public final T asObjectCopy()
    {
        return temporal();
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V with( TemporalAdjuster adjuster )
    {
        return replacement( (T) temporal().with( adjuster ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V plus( TemporalAmount amount )
    {
        return replacement( (T) temporal().plus( amount ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V minus( TemporalAmount amount )
    {
        return replacement( (T) temporal().minus( amount ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V minus( long amountToSubtract, TemporalUnit unit )
    {
        return replacement( (T) temporal().minus( amountToSubtract, unit ) );
    }

    @Override
    public final boolean isSupported( TemporalUnit unit )
    {
        return temporal().isSupported( unit );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V with( TemporalField field, long newValue )
    {
        return replacement( (T) temporal().with( field, newValue ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V plus( long amountToAdd, TemporalUnit unit )
    {
        return replacement( (T) temporal().plus( amountToAdd, unit ) );
    }

    @Override
    public final long until( Temporal endExclusive, TemporalUnit unit )
    {
        return temporal().until( endExclusive, unit );
    }

    @Override
    public final ValueRange range( TemporalField field )
    {
        return temporal().range( field );
    }

    @Override
    public final int get( TemporalField field )
    {
        return temporal().get( field );
    }

    @Override
    public <R> R query( TemporalQuery<R> query )
    {
        return temporal().query( query );
    }

    @Override
    public final boolean isSupported( TemporalField field )
    {
        return temporal().isSupported( field );
    }

    @Override
    public final long getLong( TemporalField field )
    {
        return temporal().getLong( field );
    }

    @Override
    public final NumberType numberType()
    {
        return NO_NUMBER;
    }

    @Override
    public final String toString()
    {
        return getClass().getSimpleName() + "<" + prettyPrint() + ">";
    }

    @Override
    public final boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public final boolean equals( long x )
    {
        return false;
    }

    @Override
    public final boolean equals( double x )
    {
        return false;
    }

    @Override
    public final boolean equals( char x )
    {
        return false;
    }

    @Override
    public final boolean equals( String x )
    {
        return false;
    }

    static <VALUE> VALUE parse( Class<VALUE> type, Pattern pattern, Function<Matcher,VALUE> parser, CharSequence text )
    {
        Matcher matcher = pattern.matcher( text );
        VALUE result = matcher.matches() ? parser.apply( matcher ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text, 0 );
        }
        return result;
    }

    static <VALUE> VALUE parse( Class<VALUE> type, Pattern pattern, Function<Matcher,VALUE> parser, TextValue text )
    {
        Matcher matcher = text.matcher( pattern );
        VALUE result = matcher != null && matcher.matches() ? parser.apply( matcher ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text.stringValue(), 0 );
        }
        return result;
    }

    static <VALUE> VALUE parse(
            Class<VALUE> type,
            Pattern pattern,
            BiFunction<Matcher,Supplier<ZoneId>,VALUE> parser,
            CharSequence text,
            Supplier<ZoneId> defaultZone )
    {
        Matcher matcher = pattern.matcher( text );
        VALUE result = matcher.matches() ? parser.apply( matcher, defaultZone ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text, 0 );
        }
        return result;
    }

    static <VALUE> VALUE parse(
            Class<VALUE> type,
            Pattern pattern,
            BiFunction<Matcher,Supplier<ZoneId>,VALUE> parser,
            TextValue text,
            Supplier<ZoneId> defaultZone )
    {
        Matcher matcher = text.matcher( pattern );
        VALUE result = matcher != null && matcher.matches() ? parser.apply( matcher, defaultZone ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text.stringValue(), 0 );
        }
        return result;
    }

    private static <VALUE> String valueName( Class<VALUE> type )
    {
        String name = type.getSimpleName();
        return name.substring( 0, name.length() - /*"Value" is*/5/*characters*/ );
    }
}
