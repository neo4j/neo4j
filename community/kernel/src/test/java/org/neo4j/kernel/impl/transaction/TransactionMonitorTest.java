/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class TransactionMonitorTest
{
    @Test
    public void shouldCountCommittedTransactions() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            TransactionCounters monitor = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            long startedBefore = monitor.getNumberOfStartedTransactions();
            long committedBefore = monitor.getNumberOfCommittedTransactions();
            long rolledBackBefore = monitor.getNumberOfRolledbackTransactions();
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                tx.success();
            }
            assertEquals( startedBefore+1, monitor.getNumberOfStartedTransactions() );
            assertEquals( committedBefore+1, monitor.getNumberOfCommittedTransactions() );
            assertEquals( rolledBackBefore, monitor.getNumberOfRolledbackTransactions() );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldNotCountRolledBackTransactions() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            TransactionCounters monitor = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            long startedBefore = monitor.getNumberOfStartedTransactions();
            long committedBefore = monitor.getNumberOfCommittedTransactions();
            long rolledBackBefore = monitor.getNumberOfRolledbackTransactions();
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                tx.failure();
            }
            assertEquals( startedBefore+1, monitor.getNumberOfStartedTransactions() );
            assertEquals( committedBefore, monitor.getNumberOfCommittedTransactions() );
            assertEquals( rolledBackBefore+1, monitor.getNumberOfRolledbackTransactions() );
        }
        finally
        {
            db.shutdown();
        }
    }
}
