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
package org.neo4j.kernel.impl.proc.temporal;

import java.time.Clock;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.singletonList;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public abstract class TemporalFunction<T extends AnyValue> implements CallableUserFunction
{
    public static void registerTemporalFunctions( Procedures procedures ) throws ProcedureException
    {
        register( new DateTimeFunction(), procedures );
        register( new LocalDateTimeFunction(), procedures );
        register( new DateFunction(), procedures );
        register( new TimeFunction(), procedures );
        register( new LocalTimeFunction(), procedures );
        DurationFunction.register( procedures );
    }

    private static final Key<Clock> DEFAULT_CLOCK = Context.STATEMENT_CLOCK;

    protected abstract T now( Clock clock, String timezone );

    protected abstract T parse( TextValue value, Supplier<ZoneId> defaultZone );

    protected abstract T build( MapValue map, Supplier<ZoneId> defaultZone );

    protected abstract T select( AnyValue from, Supplier<ZoneId> defaultZone );

    protected abstract T positionalCreate( AnyValue[] input );

    protected abstract T truncate( TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone );

    private static final List<FieldSignature> INPUT_SIGNATURE = singletonList( inputField(
            "input", Neo4jTypes.NTAny, nullValue( Neo4jTypes.NTAny ) ) );
    private static final String[] ALLOWED = {};
    private final UserFunctionSignature signature;

    TemporalFunction( Neo4jTypes.AnyType result )
    {
        String basename = basename( getClass() );
        assert result.getClass().getSimpleName().equals( basename + "Type" ) : "result type should match function name";
        Description description = getClass().getAnnotation( Description.class );
        this.signature = new UserFunctionSignature(
                new QualifiedName( new String[0], basename.toLowerCase() ),
                INPUT_SIGNATURE, result, null, ALLOWED,
                description == null ? null : description.value() );
    }

    private static void register( TemporalFunction<?> base, Procedures procedures ) throws ProcedureException
    {
        procedures.register( base );
        procedures.register( new Now<>( base, "transaction" ) );
        procedures.register( new Now<>( base, "statement" ) );
        procedures.register( new Now<>( base, "realtime" ) );
        procedures.register( new Truncate<>( base ) );
        base.registerMore( procedures );
    }

    private static String basename( Class<? extends TemporalFunction> function )
    {
        return function.getSimpleName().replace( "Function", "" );
    }

    static int anInt( String name, AnyValue value )
    {
        if ( value instanceof IntegralValue )
        {
            long v = ((IntegralValue) value).longValue();
            if ( v <= Integer.MAX_VALUE && v >= Integer.MIN_VALUE )
            {
                return (int) v;
            }
        }
        throw new IllegalArgumentException( name + " should be an int, not: " + value );
    }

    static String aString( String name, AnyValue value )
    {
        if ( value instanceof TextValue )
        {
            return ((TextValue) value).stringValue();
        }
        throw new IllegalArgumentException( name + " should be a string, not: " + value );
    }

    void registerMore( Procedures procedures ) throws ProcedureException
    {
        // Empty by default
    }

    @Override
    public final UserFunctionSignature signature()
    {
        return signature;
    }

    @Override
    public final T apply( Context ctx, AnyValue[] input ) throws ProcedureException
    {
        if ( input == null || input.length == 0 || input[0] == NO_VALUE || input[0] == null )
        {
            return now( ctx.get( DEFAULT_CLOCK ), null );
        }
        else if ( input.length > 1 )
        {
            return positionalCreate( input );
        }
        else if ( input[0] instanceof TextValue )
        {
            return parse( (TextValue) input[0], defaultZone( ctx ) );
        }
        else if ( input[0] instanceof TemporalValue )
        {
            return select( input[0], defaultZone( ctx ) );
        }
        else if ( input[0] instanceof MapValue )
        {
            MapValue map = (MapValue) input[0];
            String timezone = onlyTimezone( map );
            if ( timezone != null )
            {
                return now( ctx.get( DEFAULT_CLOCK ), timezone );
            }
            return build( map, defaultZone( ctx ) );
        }
        else
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature" );
        }
    }

    private static Supplier<ZoneId> defaultZone( Context ctx ) throws ProcedureException
    {
        Clock clock = ctx.get( DEFAULT_CLOCK );
        return clock::getZone;
    }

    private static String onlyTimezone( MapValue map )
    {
        if ( map.size() == 1 )
        {
            String key = single( map.keySet() );
            if ( "timezone".equalsIgnoreCase( key ) )
            {
                AnyValue timezone = map.get( key );
                if ( timezone instanceof TextValue )
                {
                    return ((TextValue) timezone).stringValue();
                }
            }
        }
        return null;
    }

    private abstract static class SubFunction<T extends AnyValue> implements CallableUserFunction
    {
        private final UserFunctionSignature signature;
        final TemporalFunction<T> function;

        SubFunction( TemporalFunction<T> base, String name, List<FieldSignature> input, String description )
        {
            this.function = base;
            this.signature = new UserFunctionSignature(
                    new QualifiedName( new String[] {base.signature.name().name()}, name ),
                    input, base.signature.outputType(), null, ALLOWED,  description  );
        }

        @Override
        public final UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public abstract T apply( Context ctx, AnyValue[] input ) throws ProcedureException;
    }

    private static class Now<T extends AnyValue> extends SubFunction<T>
    {
        private static final List<FieldSignature> SIGNATURE = singletonList( inputField(
                "timezone", Neo4jTypes.NTString, nullValue( Neo4jTypes.NTString ) ) );
        private final Key<Clock> key;

        Now( TemporalFunction<T> function, String clock )
        {
            super( function, clock, SIGNATURE, String.format(
                    "Get the current %s instant using the %s clock.",
                    basename( function.getClass() ), clock ) );
            switch ( clock )
            {
            case "transaction":
                this.key = Context.TRANSACTION_CLOCK;
                break;
            case "statement":
                this.key = Context.STATEMENT_CLOCK;
                break;
            case "realtime":
                this.key = Context.SYSTEM_CLOCK;
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized clock: " + clock );
            }
        }

        @Override
        public T apply( Context ctx, AnyValue[] input ) throws ProcedureException
        {
            if ( input == null || input.length == 0 ||
                    ((input[0] == NO_VALUE || input[0] == null) && input.length == 1) )
            {
                return function.now( ctx.get( key ), null );
            }
            else if ( input.length == 1 && input[0] instanceof TextValue )
            {
                TextValue timezone = (TextValue) input[0];
                return function.now( ctx.get( key ), timezone.stringValue() );
            }
            else
            {
                throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature" );
            }
        }
    }

    private static class Truncate<T extends AnyValue> extends SubFunction<T>
    {
        private static final List<FieldSignature> SIGNATURE = Arrays.asList(
                inputField( "unit", Neo4jTypes.NTString ),
                inputField( "input", Neo4jTypes.NTAny ),
                inputField( "fields", Neo4jTypes.NTMap, nullValue( Neo4jTypes.NTMap ) ) );

        Truncate( TemporalFunction<T> function )
        {
            super( function, "truncate", SIGNATURE, String.format(
                    "Truncate the input temporal value to a %s instant using the specified unit.",
                    basename( function.getClass() ) ) );
        }

        @Override
        public T apply( Context ctx, AnyValue[] args ) throws ProcedureException
        {
            if ( args != null && args.length >= 2 && args.length <= 3 )
            {
                AnyValue unit = args[0];
                AnyValue input = args[1];
                AnyValue fields = args.length == 2 || args[2] == NO_VALUE ? EMPTY_MAP : args[2];
                if ( unit instanceof TextValue && input instanceof TemporalValue && fields instanceof MapValue )
                {
                    return function.truncate(
                            unit( ((TextValue) unit).stringValue() ),
                            (TemporalValue)input,
                            (MapValue) fields,
                            defaultZone( ctx ) );
                }
            }
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature" );
        }

        private static TemporalUnit unit( String unit )
        {
            switch ( unit )
            {
            case "millennium":
                return ChronoUnit.MILLENNIA;
            case "century":
                return ChronoUnit.CENTURIES;
            case "decade":
                return ChronoUnit.DECADES;
            case "year":
                return ChronoUnit.YEARS;
            case "weekYear":
                return IsoFields.WEEK_BASED_YEARS;
            case "quarter":
                return IsoFields.QUARTER_YEARS;
            case "month":
                return ChronoUnit.MONTHS;
            case "week":
                return ChronoUnit.WEEKS;
            case "day":
                return ChronoUnit.DAYS;
            case "hour":
                return ChronoUnit.HOURS;
            case "minute":
                return ChronoUnit.MINUTES;
            case "second":
                return ChronoUnit.SECONDS;
            case "millisecond":
                return ChronoUnit.MILLIS;
            case "microsecond":
                return ChronoUnit.MICROS;
            default:
                throw new IllegalArgumentException( "Unsupported unit: " + unit );
            }
        }
    }
}
