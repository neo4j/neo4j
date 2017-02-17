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
package org.neo4j.kernel.impl.proc;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.procedure.MarkForTermination;

import static org.neo4j.kernel.api.proc.Context.KERNEL_TRANSACTION;


public class MarkForTerminationProvider implements ComponentRegistry.Provider<MarkForTermination>
{
    @Override
    public MarkForTermination apply( Context ctx ) throws ProcedureException
    {
        KernelTransaction ktx = ctx.get( KERNEL_TRANSACTION );
        return new TerminationTransaction( ktx );
    }

    private class TerminationTransaction implements MarkForTermination
    {
        private final KernelTransaction ktx;

        TerminationTransaction( KernelTransaction ktx )
        {
            this.ktx = ktx;
        }

        @Override
        public void mark()
        {
            ktx.markForTermination( Status.Transaction.Terminated );
        }
    }
}
