/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.proc;

import org.neo4j.kernel.api.exceptions.ProcedureException;

public interface CallableUserFunction
{
    UserFunctionSignature signature();
    Object apply( Context ctx, Object[] input ) throws ProcedureException;

    abstract class BasicUserFunction implements CallableUserFunction
    {
        private final UserFunctionSignature signature;

        protected BasicUserFunction( UserFunctionSignature signature )
        {
            this.signature = signature;
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public abstract Object apply( Context ctx, Object[] input ) throws ProcedureException;
    }
}
