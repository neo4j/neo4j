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
package org.neo4j.procedure.impl;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.procedure.TerminationGuard;

public class TerminationGuardProvider implements ThrowingFunction<Context, TerminationGuard, ProcedureException> {
    @Override
    public TerminationGuard apply(Context ctx) throws ProcedureException {
        return new TransactionTerminationGuard(ctx.kernelTransaction());
    }

    private static class TransactionTerminationGuard implements TerminationGuard {
        private final AssertOpen assertOpen;

        TransactionTerminationGuard(AssertOpen ktx) {
            this.assertOpen = ktx;
        }

        @Override
        public void check() {
            assertOpen.assertOpen();
        }
    }
}
