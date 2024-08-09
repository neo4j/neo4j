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
package org.neo4j.kernel.api.procedure;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Arrays;
import java.util.Optional;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.values.AnyValue;

public interface CallableProcedure {
    ProcedureSignature signature();

    ResourceRawIterator<AnyValue[], ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException;

    abstract class BasicProcedure implements CallableProcedure {
        private final ProcedureSignature signature;

        protected BasicProcedure(ProcedureSignature signature) {
            this.signature = signature;
        }

        @Override
        public ProcedureSignature signature() {
            return signature;
        }

        @Override
        public abstract ResourceRawIterator<AnyValue[], ProcedureException> apply(
                Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException;

        protected static <T extends AnyValue> Optional<T> getOptionalParameter(
                int parameterIndex, Class<T> parameterClass, String parameterName, AnyValue[] input)
                throws ProcedureException {
            try {
                checkRange(input, parameterIndex);
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }

            var value = input[parameterIndex];
            if (value == NO_VALUE) {
                return Optional.empty();
            }

            checkType(parameterClass, parameterName, value);

            return Optional.of(parameterClass.cast(value));
        }

        protected static <T extends AnyValue> T getParameter(
                int parameterIndex, Class<T> parameterClass, String parameterName, AnyValue[] input)
                throws ProcedureException {

            checkRange(input, parameterIndex);

            var value = input[parameterIndex];

            checkType(parameterClass, parameterName, value);

            return parameterClass.cast(value);
        }

        private static void checkRange(AnyValue[] input, int parameterIndex) {
            if (input.length == 0) {
                throw new IllegalArgumentException("Illegal input:" + Arrays.toString(input));
            }
            if (parameterIndex < 0 || parameterIndex >= input.length) {
                throw new IllegalArgumentException("Input should contains " + (parameterIndex + 1) + " parameters");
            }
        }

        private static <T> void checkType(Class<T> parameterClass, String parameterName, AnyValue value) {
            if (!parameterClass.isInstance(value)) {
                throw new IllegalArgumentException(format(
                        "Parameter '%s' should have a '%s' representation. Instead it was '%s'",
                        parameterName, parameterClass.getSimpleName(), value.getTypeName()));
            }
        }
    }
}
