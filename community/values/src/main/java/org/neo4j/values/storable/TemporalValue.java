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
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.time.temporal.ValueRange;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;

import static org.neo4j.values.storable.DateTimeValue.parseZoneName;
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

    abstract static class Builder<Input, Result> implements StructureBuilder<Input,Result>
    {
        private BuilderState<Input> state;
        private Input timezone;

        @Override
        public final Result build()
        {
            if ( state == null )
            {
                throw new IllegalArgumentException( "Builder state empty" );
            }
            return state.build( this );
        }

        @Override
        public final StructureBuilder<Input,Result> add( String fieldName, Input value )
        {
            Field field = Field.fields.get( fieldName.toLowerCase() );
            if ( field == null )
            {
                throw new IllegalArgumentException( "No such field: " + fieldName );
            }
            field.assign( this, value );
            return this;
        }

        @SuppressWarnings( "BooleanMethodIsAlwaysInverted" )
        private boolean supports( TemporalField field )
        {
            if ( field.isDateBased() )
            {
                return supportsDate();
            }
            if ( field.isTimeBased() )
            {
                return supportsTime();
            }
            throw new IllegalStateException( "Fields should be either date based or time based" );
        }

        protected abstract boolean supportsDate();

        protected abstract boolean supportsTime();

        protected abstract ZoneId timezone( Input timezone );

        // Selection

        protected abstract Result selectDateTime( Input temporal );

        protected abstract Result selectDateAndTime( Input date, Input time );

        protected abstract Result selectDateWithConstructedTime(
                Input date,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond );

        protected abstract Result selectDate( Input temporal );

        protected abstract Result selectTime( Input temporal );

        // Construction

        protected abstract Result constructYear( Input year );

        protected abstract Result constructTime(
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond );

        // - by calendar date

        protected abstract Result constructCalendarDate( Input year, Input month, Input day );

        protected abstract Result constructCalendarDateWithSelectedTime(
                Input year, Input month, Input day, Input time );

        protected abstract Result constructCalendarDateWithConstructedTime(
                Input year, Input month, Input day,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond );

        // - by week date

        protected abstract Result constructWeekDate( Input year, Input week, Input dayOfWeek );

        protected abstract Result constructWeekDateWithSelectedTime(
                Input year, Input week, Input dayOfWeek, Input time );

        protected abstract Result constructWeekDateWithConstructedTime(
                Input year, Input week, Input dayOfWeek,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond );

        // - by quarter date

        protected abstract Result constructQuarterDate( Input year, Input quarter, Input dayOfQuarter );

        protected abstract Result constructQuarterDateWithSelectedTime(
                Input year, Input quarter, Input dayOfQuarter, Input time );

        protected abstract Result constructQuarterDateWithConstructedTime(
                Input year, Input quarter, Input dayOfQuarter,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond );

        // - by ordinal date

        protected abstract Result constructOrdinalDate( Input year, Input ordinalDay );

        protected abstract Result constructOrdinalDateWithSelectedTime( Input year, Input ordinalDay, Input time );

        protected abstract Result constructOrdinalDateWithConstructedTime(
                Input year, Input ordinalDay,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond );

        protected final ZoneId optionalTimezone()
        {
            return timezone == null ? null : timezone();
        }

        protected final ZoneId timezone()
        {
            return timezone( timezone );
        }

        protected final ZoneId timezoneOf( AnyValue timezone )
        {
            if ( timezone instanceof TextValue )
            {
                return parseZoneName( ((TextValue) timezone).stringValue() );
            }
            throw new UnsupportedOperationException( "Cannot convert to ZoneId: " + timezone );
        }
    }

    private enum Field
    {
        year( ChronoField.YEAR ),
        quarter( IsoFields.QUARTER_OF_YEAR ),
        month( ChronoField.MONTH_OF_YEAR ),
        week( IsoFields.WEEK_OF_WEEK_BASED_YEAR ),
        ordinalDay( ChronoField.DAY_OF_YEAR ),
        dayOfQuarter( IsoFields.DAY_OF_QUARTER ),
        dayOfWeek( ChronoField.DAY_OF_WEEK ),
        day( ChronoField.DAY_OF_MONTH ),
        hour( ChronoField.HOUR_OF_DAY ),
        minute( ChronoField.MINUTE_OF_HOUR ),
        second( ChronoField.SECOND_OF_MINUTE ),
        millisecond( ChronoField.MILLI_OF_SECOND ),
        microsecond( ChronoField.MICRO_OF_SECOND ),
        nanosecond( ChronoField.NANO_OF_SECOND ),
        timezone//<pre>
        { //</pre>

            @Override
            <Input> void assign( Builder<Input,?> builder, Input value )
            {
                if ( builder.timezone != null )
                {
                    throw new IllegalArgumentException( "Cannot assign timezone twice." );
                }
                builder.timezone = value;
            }
        },
        // group selectors
        date//<pre>
        { //</pre>

            @Override
            <Input> void assign( Builder<Input,?> builder, Input value )
            {
                if ( builder.state == null )
                {
                    builder.state = new DateTimeBuilder<>();
                }
                builder.state = builder.state.date( value );
            }
        },
        time//<pre>
        { //</pre>

            @Override
            <Input> void assign( Builder<Input,?> builder, Input value )
            {
                if ( builder.state == null )
                {
                    builder.state = new DateTimeBuilder<>();
                }
                builder.state = builder.state.time( value );
            }
        },
        datetime//<pre>
        { //</pre>

            @Override
            <Input> void assign( Builder<Input,?> builder, Input value )
            {
                if ( builder.state == null )
                {
                    builder.state = new SelectDateTime<>( value );
                }
                else
                {
                    throw new IllegalArgumentException( "Cannot select datetime when assigning other fields." );
                }
            }
        };
        private static final Map<String,Field> fields = new HashMap<>();

        static
        {
            for ( Field field : values() )
            {
                fields.put( field.name().toLowerCase(), field );
            }
            // aliases
            fields.put( "weekday", dayOfWeek );
            fields.put( "quarterday", dayOfQuarter );
        }

        private final TemporalField field;

        Field( TemporalField field )
        {
            this.field = field;
        }

        Field()
        {
            this.field = null;
        }

        <Input> void assign( Builder<Input,?> builder, Input value )
        {
            assert field != null : "method should have been overridden";
            if ( !builder.supports( field ) )
            {
                throw new IllegalArgumentException( "Not supported: " + name() );
            }
            if ( builder.state == null )
            {
                builder.state = new DateTimeBuilder<>();
            }
            builder.state = builder.state.assign( this, value );
        }
    }

    private abstract static class BuilderState<Input>
    {
        abstract BuilderState<Input> assign( Field field, Input value );

        abstract BuilderState<Input> date( Input date );

        abstract BuilderState<Input> time( Input time );

        abstract <Result> Result build( Builder<Input,Result> builder );
    }

    private static final class SelectDateTime<Input> extends BuilderState<Input>
    {
        private final Input datetime;

        SelectDateTime( Input temporal )
        {
            this.datetime = temporal;
        }

        @Override
        BuilderState<Input> assign( Field field, Input value )
        {
            throw new IllegalArgumentException( "Cannot assign " + field + " when selecting datetime." );
        }

        @Override
        BuilderState<Input> date( Input date )
        {
            throw new IllegalArgumentException( "Cannot select date when selecting datetime." );
        }

        @Override
        BuilderState<Input> time( Input time )
        {
            throw new IllegalArgumentException( "Cannot select time when selecting datetime." );
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.selectDateTime( datetime );
        }
    }

    private static final class DateTimeBuilder<Input> extends BuilderState<Input>
    {
        private DateBuilder<Input> date;
        private TimeBuilder<Input> time;

        @Override
        BuilderState<Input> assign( Field field, Input value )
        {
            if ( field.field.isDateBased() )
            {
                if ( date == null )
                {
                    date = new ConstructDate<>();
                }
                date = date.assign( field, value );
            }
            else
            {
                if ( time == null )
                {
                    time = new ConstructTime<>();
                }
                time.assign( field, value );
            }
            return this;
        }

        @Override
        BuilderState<Input> date( Input date )
        {
            if ( this.date != null )
            {
                throw new IllegalArgumentException( "cannot select date when also assigning date" );
            }
            this.date = new SelectDate<>( date );
            return this;
        }

        @Override
        BuilderState<Input> time( Input time )
        {
            if ( this.time != null )
            {
                throw new IllegalArgumentException( "cannot select time when also assigning time" );
            }
            this.time = new SelectTime<>( time );
            return this;
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            if ( time == null )
            {
                return date.build( builder );
            }
            else if ( date == null )
            {
                return time.build( builder );
            }
            else
            {
                return time.build( builder, date );
            }
        }
    }

    private abstract static class DateBuilder<Input>
    {
        abstract DateBuilder<Input> assign( Field field, Input value );

        abstract <Result> Result build( Builder<Input,Result> builder );

        abstract <Result> Result selectTime( Input time, Builder<Input,Result> builder );

        abstract <Result> Result constructTime(
                Builder<Input,Result> builder,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond );
    }

    private abstract static class TimeBuilder<Input>
    {
        abstract void assign( Field field, Input value );

        abstract <Result> Result build( Builder<Input,Result> builder );

        abstract <Result> Result build( Builder<Input,Result> builder, DateBuilder<Input> date );
    }

    private static final class SelectDate<Input> extends DateBuilder<Input>
    {
        private final Input temporal;

        SelectDate( Input temporal )
        {
            this.temporal = temporal;
        }

        @Override
        SelectDate<Input> assign( Field field, Input value )
        {
            throw new IllegalArgumentException( "cannot assign " + field + " when selecting date" );
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.selectDate( temporal );
        }

        @Override
        <Result> Result selectTime( Input time, Builder<Input,Result> builder )
        {
            return builder.selectDateAndTime( temporal, time );
        }

        @Override
        <Result> Result constructTime(
                Builder<Input,Result> builder,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            return builder.selectDateWithConstructedTime(
                    temporal,
                    hour,
                    minute,
                    second,
                    millisecond,
                    microsecond,
                    nanosecond );
        }
    }

    private static final class SelectTime<Input> extends TimeBuilder<Input>
    {
        private final Input temporal;

        SelectTime( Input temporal )
        {
            this.temporal = temporal;
        }

        @Override
        void assign( Field field, Input value )
        {
            throw new IllegalArgumentException( "cannot assign " + field + " when selecting time" );
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.selectTime( temporal );
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder, DateBuilder<Input> date )
        {
            return date.selectTime( temporal, builder );
        }
    }

    private static final class ConstructTime<Input> extends TimeBuilder<Input>
    {
        private Input hour;
        private Input minute;
        private Input second;
        private Input millisecond;
        private Input microsecond;
        private Input nanosecond;

        @Override
        void assign( Field field, Input value )
        {
            switch ( field )
            {
            case hour:
                hour = assignment( field, hour, value );
                break;
            case minute:
                minute = assignment( field, minute, value );
                break;
            case second:
                second = assignment( field, second, value );
                break;
            case millisecond:
                millisecond = assignment( field, millisecond, value );
                break;
            case microsecond:
                microsecond = assignment( field, microsecond, value );
                break;
            case nanosecond:
                nanosecond = assignment( field, nanosecond, value );
                break;
            default:
                throw new IllegalStateException( "Not a time field: " + field );
            }
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {

            return builder.constructTime( hour, minute, second, millisecond, microsecond, nanosecond );
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder, DateBuilder<Input> date )
        {
            return date.constructTime( builder, hour, minute, second, millisecond, microsecond, nanosecond );
        }
    }

    private static class ConstructDate<Input> extends DateBuilder<Input>
    {
        Input year;

        @Override
        ConstructDate<Input> assign( Field field, Input value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case quarter:
            case dayOfQuarter:
                return new QuarterDate<>( year ).assign( field, value );
            case month:
            case day:
                return new CalendarDate<>( year ).assign( field, value );
            case week:
            case dayOfWeek:
                return new WeekDate<>( year ).assign( field, value );
            case ordinalDay:
                return new OrdinalDate<>( year ).assign( field, value );
            default:
                throw new IllegalStateException( "Not a date field: " + field );
            }
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.constructYear( year );
        }

        @Override
        <Result> Result selectTime( Input time, Builder<Input,Result> builder )
        {
            throw new IllegalStateException( "Cannot specify time for a year." );
        }

        @Override
        <Result> Result constructTime(
                Builder<Input,Result> builder,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            throw new IllegalStateException( "Cannot specify time for a year." );
        }
    }

    private static final class CalendarDate<Input> extends ConstructDate<Input>
    {
        private Input month;
        private Input day;

        CalendarDate( Input year )
        {
            this.year = year;
        }

        @Override
        ConstructDate<Input> assign( Field field, Input value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case month:
                month = assignment( field, month, value );
                return this;
            case day:
                day = assignment( field, day, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to calendar date." );
            }
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.constructCalendarDate( year, month, day );
        }

        @Override
        <Result> Result selectTime( Input time, Builder<Input,Result> builder )
        {
            return builder.constructCalendarDateWithSelectedTime( year, month, day, time );
        }

        @Override
        <Result> Result constructTime(
                Builder<Input,Result> builder,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            return builder.constructCalendarDateWithConstructedTime( year, month, day,
                    hour, minute, second, millisecond, microsecond, nanosecond );
        }
    }

    private static final class WeekDate<Input> extends ConstructDate<Input>
    {
        private Input week;
        private Input dayOfWeek;

        WeekDate( Input year )
        {
            this.year = year;
        }

        @Override
        ConstructDate<Input> assign( Field field, Input value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case week:
                week = assignment( field, week, value );
                return this;
            case dayOfWeek:
                dayOfWeek = assignment( field, dayOfWeek, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to week date." );
            }
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.constructWeekDate( year, week, dayOfWeek );
        }

        @Override
        <Result> Result selectTime( Input time, Builder<Input,Result> builder )
        {
            return builder.constructWeekDateWithSelectedTime( year, week, dayOfWeek, time );
        }

        @Override
        <Result> Result constructTime(
                Builder<Input,Result> builder,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            return builder.constructWeekDateWithConstructedTime( year, week, dayOfWeek,
                    hour, minute, second, millisecond, microsecond, nanosecond );
        }
    }

    private static final class QuarterDate<Input> extends ConstructDate<Input>
    {
        private Input quarter;
        private Input dayOfQuarter;

        QuarterDate( Input year )
        {
            this.year = year;
        }

        @Override
        ConstructDate<Input> assign( Field field, Input value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case quarter:
                quarter = assignment( field, quarter, value );
                return this;
            case dayOfQuarter:
                dayOfQuarter = assignment( field, dayOfQuarter, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to quarter date." );
            }
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.constructQuarterDate( year, quarter, dayOfQuarter );
        }

        @Override
        <Result> Result selectTime( Input time, Builder<Input,Result> builder )
        {
            return builder.constructQuarterDateWithSelectedTime( year, quarter, dayOfQuarter, time );
        }

        @Override
        <Result> Result constructTime(
                Builder<Input,Result> builder,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            return builder.constructQuarterDateWithConstructedTime( year, quarter, dayOfQuarter,
                    hour, minute, second, millisecond, microsecond, nanosecond );
        }
    }

    private static final class OrdinalDate<Input> extends ConstructDate<Input>
    {
        private Input ordinalDay;

        OrdinalDate( Input year )
        {
            this.year = year;
        }

        @Override
        ConstructDate<Input> assign( Field field, Input value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case ordinalDay:
                ordinalDay = assignment( field, ordinalDay, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to ordinal date." );
            }
        }

        @Override
        <Result> Result build( Builder<Input,Result> builder )
        {
            return builder.constructOrdinalDate( year, ordinalDay );
        }

        @Override
        <Result> Result selectTime( Input time, Builder<Input,Result> builder )
        {
            return builder.constructOrdinalDateWithSelectedTime( year, ordinalDay, time );
        }

        @Override
        <Result> Result constructTime(
                Builder<Input,Result> builder,
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            return builder.constructOrdinalDateWithConstructedTime( year, ordinalDay,
                    hour, minute, second, millisecond, microsecond, nanosecond );
        }
    }

    private static <Input> Input assignment( Field field, Input oldValue, Input newValue )
    {
        if ( oldValue != null )
        {
            throw new IllegalArgumentException( "cannot re-assign " + field );
        }
        return newValue;
    }
}
