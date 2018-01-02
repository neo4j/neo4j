/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.workload;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.stresstests.transaction.checkpoint.mutation.RandomMutation;

class Worker implements Runnable
{
    interface Monitor
    {
        void transactionCompleted();
        boolean stop();
        void done();
    }

    private final GraphDatabaseService db;
    private final RandomMutation randomMutation;
    private final Monitor monitor;
    private final int numOpsPerTx;

    public Worker( GraphDatabaseService db, RandomMutation randomMutation, Monitor monitor, int numOpsPerTx )
    {
        this.db = db;
        this.randomMutation = randomMutation;
        this.monitor = monitor;
        this.numOpsPerTx = numOpsPerTx;
    }

    @Override
    public void run()
    {
        do
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < numOpsPerTx; i++ )
                {
                    randomMutation.perform();
                }
                tx.success();
            }
            catch ( DeadlockDetectedException ignore )
            {
                // simply give up
            }
            catch ( Exception e )
            {
                // ignore and go on
                e.printStackTrace();
            }

            monitor.transactionCompleted();
        }
        while ( !monitor.stop() );

        monitor.done();
    }
}
