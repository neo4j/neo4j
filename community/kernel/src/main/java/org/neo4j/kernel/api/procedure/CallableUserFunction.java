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

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.values.AnyValue;

public interface CallableUserFunction {
    UserFunctionSignature signature();

    /**
     * Call the user function
     *
     * @param ctx the context used for the call
     * @param input the input arguments, note that the array may be mutated at a later stage so defensive copying must be applied if you need to keep old inputs.
     * @return the result of the function
     * @throws ProcedureException
     */
    AnyValue apply(Context ctx, AnyValue[] input) throws ProcedureException;

    abstract class BasicUserFunction implements CallableUserFunction {
        private final UserFunctionSignature signature;

        protected BasicUserFunction(UserFunctionSignature signature) {
            this.signature = signature;
        }

        @Override
        public UserFunctionSignature signature() {
            return signature;
        }
    }
}
