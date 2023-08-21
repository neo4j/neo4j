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
package org.neo4j.kernel.api;

import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.test.Race;

class KernelAPIParallelStress {
    static <RESOURCE extends AutoCloseable> void parallelStressInTx(
            Kernel kernel,
            int nThreads,
            Function<KernelTransaction, RESOURCE> resourceSupplier,
            BiFunction<Read, RESOURCE, Runnable> runnable)
            throws Throwable {
        Race race = new Race();

        List<RESOURCE> resources = new ArrayList<>();
        try (KernelTransaction tx = kernel.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED)) {
            // assert our test works single-threaded before racing
            try (RESOURCE cursor = resourceSupplier.apply(tx)) {
                runnable.apply(tx.dataRead(), cursor).run();
            }

            for (int i = 0; i < nThreads; i++) {
                final RESOURCE resource = resourceSupplier.apply(tx);

                race.addContestant(runnable.apply(tx.dataRead(), resource), 1);

                resources.add(resource);
            }

            race.go();

            // clean-up
            closeAllUnchecked(resources);
            tx.commit();
        }
    }
}
