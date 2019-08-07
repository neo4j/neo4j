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
package org.neo4j.kernel.api;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.test.Race;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;

class KernelAPIParallelStress
{
    static <RESOURCE extends AutoCloseable> void parallelStressInTx( Kernel kernel,
                                                                     int nThreads,
                                                                     Function<Transaction, RESOURCE> resourceSupplier,
                                                                     BiFunction<Read, RESOURCE, Runnable> runnable ) throws Throwable
    {
        Race race = new Race();

        long endTime = currentTimeMillis() + SECONDS.toMillis( 30 );
        race.withEndCondition( () -> currentTimeMillis() > endTime );

        List<RESOURCE> nodeCursors = new ArrayList<RESOURCE>();
        try ( Transaction tx = kernel.beginTransaction( explicit, LoginContext.AUTH_DISABLED ) )
        {
            // assert our test works single-threaded before racing
            try ( RESOURCE nodeCursor = resourceSupplier.apply( tx ) )
            {
                runnable.apply( tx.dataRead(), nodeCursor ).run();
            }

            for ( int i = 0; i < nThreads; i++ )
            {
                final RESOURCE resource = resourceSupplier.apply( tx );

                race.addContestant( runnable.apply( tx.dataRead(), resource ) );

                nodeCursors.add( resource );
            }

            race.go();

            // clean-up
            for ( RESOURCE cursor : nodeCursors )
            {
                cursor.close();
            }
            tx.commit();
        }
    }
}
