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

import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.values.storable.Values.NO_VALUE;

@Description( "Construct a Duration value." )
class DurationFunction implements CallableUserFunction
{
    private static final UserFunctionSignature DURATION =
            new UserFunctionSignature(
                    new QualifiedName( new String[0], "duration" ),
                    Collections.singletonList( inputField( "input", Neo4jTypes.NTAny ) ),
                    Neo4jTypes.NTDuration, null, new String[0],
                    DurationFunction.class.getAnnotation( Description.class ).value(),
                    true );

    static void register( Procedures procedures ) throws ProcedureException
    {
        procedures.register( new DurationFunction() );
        procedures.register( new Between( "between" ) );
        procedures.register( new Between( "inMonths" ) );
        procedures.register( new Between( "inDays" ) );
        procedures.register( new Between( "inSeconds" ) );
    }

    @Override
    public UserFunctionSignature signature()
    {
        return DURATION;
    }

    @Override
    public AnyValue apply( Context ctx, AnyValue[] input ) throws ProcedureException
    {
        if ( input == null )
        {
            return NO_VALUE;
        }
        else if ( input.length == 1 )
        {
            if ( input[0] == NO_VALUE || input[0] == null )
            {
                return NO_VALUE;
            }
            else if ( input[0] instanceof TextValue )
            {
                return DurationValue.parse( (TextValue) input[0] );
            }
            else if ( input[0] instanceof MapValue )
            {
                MapValue map = (MapValue) input[0];
                return DurationValue.build( map );
            }
        }
        throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature for " + getClass().getSimpleName() +
                ": Provided input was " + Arrays.toString( input ) );
    }

    private static class Between implements CallableUserFunction
    {
        private static final String DESCRIPTION =
                "Compute the duration between the 'from' instant (inclusive) and the 'to' instant (exclusive) in %s.";
        private static final List<FieldSignature> SIGNATURE = Arrays.asList(
                inputField( "from", Neo4jTypes.NTAny ),
                inputField( "to", Neo4jTypes.NTAny ) );
        private final UserFunctionSignature signature;
        private final TemporalUnit unit;

        private Between( String unit )
        {
            String unitString;
            switch ( unit )
            {
            case "between":
                this.unit = null;
                unitString = "logical units";
                break;
            case "inMonths":
                this.unit = ChronoUnit.MONTHS;
                unitString = "months";
                break;
            case "inDays":
                this.unit = ChronoUnit.DAYS;
                unitString = "days";
                break;
            case "inSeconds":
                this.unit = ChronoUnit.SECONDS;
                unitString = "seconds";
                break;
            default:
                throw new IllegalStateException( "Unsupported unit: " + unit );
            }
            this.signature = new UserFunctionSignature(
                    new QualifiedName( new String[] {"duration"}, unit ),
                    SIGNATURE, Neo4jTypes.NTDuration, null, new String[0],
                    String.format(
                            DESCRIPTION, unitString ), true );
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public AnyValue apply( Context ctx, AnyValue[] input ) throws ProcedureException
        {
            if ( input == null || (input.length == 2 && (input[0] == NO_VALUE || input[0] == null) || input[1] == NO_VALUE || input[1] == null) )
            {
                return NO_VALUE;
            }
            else if ( input.length == 2 )
            {
                if ( input[0] instanceof TemporalValue && input[1] instanceof TemporalValue )
                {
                    TemporalValue from = (TemporalValue) input[0];
                    TemporalValue to = (TemporalValue) input[1];
                    return DurationValue.between( unit, from, to );
                }
            }
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature for " + getClass().getSimpleName() +
                ": Provided input was " + Arrays.toString( input ) );
        }
    }
}
