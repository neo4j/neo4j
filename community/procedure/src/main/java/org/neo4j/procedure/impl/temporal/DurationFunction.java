/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.impl.temporal;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.neo4j.cypher.internal.expressions.functions.Category;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

@Description("Creates a `DURATION` value.")
class DurationFunction implements CallableUserFunction {
    private static final String CATEGORY = Category.TEMPORAL();

    private static final UserFunctionSignature DURATION = new UserFunctionSignature(
            new QualifiedName(EMPTY_STRING_ARRAY, "duration"),
            Collections.singletonList(inputField("input", Neo4jTypes.NTAny)),
            Neo4jTypes.NTDuration,
            false,
            null,
            DurationFunction.class.getAnnotation(Description.class).value(),
            CATEGORY,
            true,
            true,
            false,
            true);

    static void register(GlobalProcedures globalProcedures) throws ProcedureException {
        globalProcedures.register(new DurationFunction());
        globalProcedures.register(new Between("between"));
        globalProcedures.register(new Between("inMonths"));
        globalProcedures.register(new Between("inDays"));
        globalProcedures.register(new Between("inSeconds"));
    }

    @Override
    public UserFunctionSignature signature() {
        return DURATION;
    }

    @Override
    public AnyValue apply(Context ctx, AnyValue[] input) throws ProcedureException {
        if (input == null) {
            return NO_VALUE;
        } else if (input.length == 1) {
            if (input[0] == NO_VALUE || input[0] == null) {
                return NO_VALUE;
            } else if (input[0] instanceof TextValue) {
                return DurationValue.parse((TextValue) input[0]);
            } else if (input[0] instanceof MapValue map) {
                return DurationValue.build(map);
            }
        }
        throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                "Invalid call signature for " + getClass().getSimpleName() + ": Provided input was "
                        + Arrays.toString(input));
    }

    private static class Between implements CallableUserFunction {
        private static final String DESCRIPTION =
                "Computes the `DURATION` between the `from` instant (inclusive) and the `to` instant (exclusive) in %s.";
        private static final List<FieldSignature> SIGNATURE =
                Arrays.asList(inputField("from", Neo4jTypes.NTAny), inputField("to", Neo4jTypes.NTAny));
        private final UserFunctionSignature signature;
        private final TemporalUnit unit;

        private Between(String unit) {
            String unitString;
            switch (unit) {
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
                    throw new IllegalStateException("Unsupported unit: " + unit);
            }
            this.signature = new UserFunctionSignature(
                    new QualifiedName(new String[] {"duration"}, unit),
                    SIGNATURE,
                    Neo4jTypes.NTDuration,
                    false,
                    null,
                    String.format(DESCRIPTION, unitString),
                    CATEGORY,
                    true,
                    true,
                    false,
                    true);
        }

        @Override
        public UserFunctionSignature signature() {
            return signature;
        }

        @Override
        public AnyValue apply(Context ctx, AnyValue[] input) throws ProcedureException {
            if (input == null
                    || (input.length == 2 && (input[0] == NO_VALUE || input[0] == null)
                            || input[1] == NO_VALUE
                            || input[1] == null)) {
                return NO_VALUE;
            } else if (input.length == 2) {
                if (input[0] instanceof TemporalValue from && input[1] instanceof TemporalValue to) {
                    return DurationValue.between(unit, from, to);
                }
            }
            throw new ProcedureException(
                    Status.Procedure.ProcedureCallFailed,
                    "Invalid call signature for " + getClass().getSimpleName() + ": Provided input was "
                            + Arrays.toString(input));
        }
    }
}
