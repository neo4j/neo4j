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
package org.neo4j.kernel.impl.transaction;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

@RunWith( Parameterized.class )
public class TransactionMonitorTest
{

    @Parameterized.Parameter( 0 )
    public ThrowingConsumer<GraphDatabaseService,Exception> dbConsumer;

    @Parameterized.Parameter( 1 )
    public boolean isWriteTx;

    @Parameterized.Parameter( 2 )
    public String ignored; // to make JUnit happy...

    @Parameterized.Parameters( name = "{2}" )
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(
                new Object[]{(ThrowingConsumer<GraphDatabaseService,Exception>) db -> {}, false, "read"},
                new Object[]{(ThrowingConsumer<GraphDatabaseService,Exception>) GraphDatabaseService::createNode,
                        true, "write"}
        );
    }

    @Test
    public void shouldCountCommittedTransactions() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            TransactionCounters counts = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            TransactionCountersChecker checker = new TransactionCountersChecker( counts );
            try ( Transaction tx = db.beginTx() )
            {
                dbConsumer.accept( db );
                tx.success();
            }
            checker.verifyCommitted( isWriteTx, counts );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shoulCountRolledBackTransactions() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            TransactionCounters counts = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            TransactionCountersChecker checker = new TransactionCountersChecker( counts );
            try ( Transaction tx = db.beginTx() )
            {
                dbConsumer.accept( db );
                tx.failure();
            }
            checker.verifyRolledBacked( isWriteTx, counts );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldCountTerminatedTransactions() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            TransactionCounters counts = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            TransactionCountersChecker checker = new TransactionCountersChecker( counts );
            try ( Transaction tx = db.beginTx() )
            {
                dbConsumer.accept( db );
                tx.terminate();
            }
            checker.verifyTerminated( isWriteTx, counts );
        }
        finally
        {
            db.shutdown();
        }
    }
}
