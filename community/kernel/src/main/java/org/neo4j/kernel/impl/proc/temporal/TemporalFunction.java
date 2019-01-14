/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.impl.proc.ProcedureConfig;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.singletonList;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public abstract class TemporalFunction<T extends AnyValue> implements CallableUserFunction
{
    private static final String DEFAULT_TEMPORAL_ARGUMENT = "DEFAULT_TEMPORAL_ARGUMENT";
    private static final TextValue DEFAULT_TEMPORAL_ARGUMENT_VALUE = Values.stringValue( DEFAULT_TEMPORAL_ARGUMENT );
    private static final DefaultParameterValue DEFAULT_PARAMETER_VALUE = new DefaultParameterValue( DEFAULT_TEMPORAL_ARGUMENT, Neo4jTypes.NTAny );

    public static void registerTemporalFunctions( Procedures procedures, ProcedureConfig procedureConfig ) throws ProcedureException
    {
        Supplier<ZoneId> defaultZone = procedureConfig::getDefaultTemporalTimeZone;
        register( new DateTimeFunction( defaultZone ), procedures );
        register( new LocalDateTimeFunction( defaultZone ), procedures );
        register( new DateFunction( defaultZone ), procedures );
        register( new TimeFunction( defaultZone ), procedures );
        register( new LocalTimeFunction( defaultZone ), procedures );
        DurationFunction.register( procedures );
    }

    private static final Key<Clock> DEFAULT_CLOCK = Context.STATEMENT_CLOCK;

    /**
     * @param clock the clock to use
     * @param timezone an explicit timezone or {@code null}. In the latter case, the defaultZone is used
     * @param defaultZone configured default time zone.
     * @return the current time/date
     */
    protected abstract T now( Clock clock, String timezone, Supplier<ZoneId> defaultZone );

    protected abstract T parse( TextValue value, Supplier<ZoneId> defaultZone );

    protected abstract T build( MapValue map, Supplier<ZoneId> defaultZone );

    protected abstract T select( AnyValue from, Supplier<ZoneId> defaultZone );

    protected abstract T truncate( TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone );

    private static final List<FieldSignature> INPUT_SIGNATURE = singletonList( inputField( "input", Neo4jTypes.NTAny, DEFAULT_PARAMETER_VALUE ) );
    private static final String[] ALLOWED = {};

    private final UserFunctionSignature signature;
    private final Supplier<ZoneId> defaultZone;

    TemporalFunction( Neo4jTypes.AnyType result, Supplier<ZoneId> defaultZone )
    {
        String basename = basename( getClass() );
        assert result.getClass().getSimpleName().equals( basename + "Type" ) : "result type should match function name";
        Description description = getClass().getAnnotation( Description.class );
        this.signature = new UserFunctionSignature(
                new QualifiedName( new String[0], basename.toLowerCase() ),
                INPUT_SIGNATURE, result, null, ALLOWED,
                description == null ? null : description.value(), true );
        this.defaultZone = defaultZone;
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
    public final AnyValue apply( Context ctx, AnyValue[] input ) throws ProcedureException
    {
        if ( input == null || (input.length > 0 && (input[0] == NO_VALUE || input[0] == null)) )
        {
            return NO_VALUE;
        }
        else if ( input.length == 0 || input[0].equals( DEFAULT_TEMPORAL_ARGUMENT_VALUE ) )
        {
            return now( ctx.get( DEFAULT_CLOCK ), null, defaultZone );
        }
        else if ( input[0] instanceof TextValue )
        {
            return parse( (TextValue) input[0], defaultZone );
        }
        else if ( input[0] instanceof TemporalValue )
        {
            return select( input[0], defaultZone );
        }
        else if ( input[0] instanceof MapValue )
        {
            MapValue map = (MapValue) input[0];
            String timezone = onlyTimezone( map );
            if ( timezone != null )
            {
                return now( ctx.get( DEFAULT_CLOCK ), timezone, defaultZone );
            }
            return build( map, defaultZone );
        }
        else
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature for " + getClass().getSimpleName() +
                    ": Provided input was " + Arrays.toString( input ) );
        }
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
                    input, base.signature.outputType(), null, ALLOWED,  description, true );
        }

        @Override
        public final UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public abstract AnyValue apply( Context ctx, AnyValue[] input ) throws ProcedureException;
    }

    private static class Now<T extends AnyValue> extends SubFunction<T>
    {
        private static final List<FieldSignature> SIGNATURE = singletonList( inputField( "timezone", Neo4jTypes.NTAny, DEFAULT_PARAMETER_VALUE ) );
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
        public AnyValue apply( Context ctx, AnyValue[] input ) throws ProcedureException
        {
            if ( input == null || (input.length > 0 && (input[0] == NO_VALUE || input[0] == null)) )
            {
                return NO_VALUE;
            }
            else if ( input.length == 0 || input[0].equals( DEFAULT_TEMPORAL_ARGUMENT_VALUE ) )
            {
                return function.now( ctx.get( key ), null, function.defaultZone );
            }
            else if ( input.length == 1 && input[0] instanceof TextValue )
            {
                TextValue timezone = (TextValue) input[0];
                return function.now( ctx.get( key ), timezone.stringValue(), function.defaultZone );
            }
            else
            {
                throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature for " + getClass().getSimpleName() +
                    ": Provided input was " + Arrays.toString( input ) );
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
                            function.defaultZone );
                }
            }
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature for " + getClass().getSimpleName() +
                    ": Provided input was " + Arrays.toString( args ) );
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
