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
package org.neo4j.kernel.impl.proc;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.procedure.TerminationGuard;

import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;

public class TerminationGuardProvider implements ComponentRegistry.Provider<TerminationGuard>
{
    private final Guard guard;

    public TerminationGuardProvider( Guard guard )
    {
        this.guard = guard;
    }

    @Override
    public TerminationGuard apply( Context ctx ) throws ProcedureException
    {
        KernelTransaction ktx = ctx.get( KERNEL_TRANSACTION );
        return new TransactionTerminationGuard( ktx );
    }

    private class TransactionTerminationGuard implements TerminationGuard
    {
        private final KernelTransaction ktx;

        TransactionTerminationGuard( KernelTransaction ktx )
        {
            this.ktx = ktx;
        }

        @Override
        public void check()
        {
            guard.check( ktx );
        }
    }
}
