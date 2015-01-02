/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;

public class TransactionMonitorTest
{
    @Test
    public void shouldCountCommittedTransactions() throws Exception
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        Monitors monitors = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Monitors.class );
        EideticTransactionMonitor monitor = new EideticTransactionMonitor();
        monitors.addMonitorListener( monitor, XaResourceManager.class.getName(), NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();

        assertEquals( 1, monitor.getCommitCount() );
        assertEquals( 0, monitor.getInjectOnePhaseCommitCount() );
        assertEquals( 0, monitor.getInjectTwoPhaseCommitCount() );
    }

    @Test
    public void shouldNotCountRolledBackTransactions() throws Exception
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        Monitors monitors = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Monitors.class );
        EideticTransactionMonitor monitor = new EideticTransactionMonitor();
        monitors.addMonitorListener( monitor, XaResourceManager.class.getName(), NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

        Transaction tx = db.beginTx();
        db.createNode();
        tx.failure();
        tx.finish();

        assertEquals( 0, monitor.getCommitCount() );
        assertEquals( 0, monitor.getInjectOnePhaseCommitCount() );
        assertEquals( 0, monitor.getInjectTwoPhaseCommitCount() );
    }
}
