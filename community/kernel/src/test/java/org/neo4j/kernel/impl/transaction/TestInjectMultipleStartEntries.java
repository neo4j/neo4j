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
package org.neo4j.kernel.impl.transaction;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.LogTestUtils.LogHookAdapter;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;

public class TestInjectMultipleStartEntries
{
    @Test
    public void fail_prepare_two_phase_transaction_should_not_yield_two_start_log_entries() throws Exception
    {
        // GIVEN
        // -- a database with one additional data source and some initial data
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() ).newImpermanentDatabase( storeDir );
        XaDataSourceManager xaDs = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class );
        XaDataSource additionalDs = new OtherDummyXaDataSource( "dummy", "dummy".getBytes(), new FakeXAResource( "dummy" ) );
        xaDs.registerDataSource( additionalDs );
        Node node = createNodeWithOneRelationshipToIt( db );

        // WHEN
        // -- doing a transaction involving both data sources and fails in prepare phase
        //    due to invalid transaction data
        deleteNodeButNotItsRelationshipsInATwoPhaseTransaction( db, additionalDs, node );
        db.shutdown();

        // THEN
        // -- the logical log should only contain unique start entries after this transaction
        filterNeostoreLogicalLog( fs.get(), new File( storeDir, LOGICAL_LOG_DEFAULT_NAME + ".v0" ),
                new VerificationLogHook() );
    }

    private final String storeDir = "dir";
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private static class VerificationLogHook extends LogHookAdapter<LogEntry>
    {
        private final Set<Xid> startXids = new HashSet<>();

        @Override
        public boolean accept( LogEntry item )
        {
            if ( item instanceof LogEntry.Start )
                assertTrue( startXids.add( ((LogEntry.Start) item).getXid() ) );
            return true;
        }
    }

    private void deleteNodeButNotItsRelationshipsInATwoPhaseTransaction( GraphDatabaseAPI db,
            XaDataSource additionalDs, Node node ) throws Exception
    {
        Transaction tx = db.beginTx();
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        TransactionManager transactionManager = dependencyResolver.resolveDependency(TransactionManager.class);
        additionalDs.getXaConnection().enlistResource( transactionManager.getTransaction() );
        node.delete();
        tx.success();
        try
        {
            tx.finish();
            fail( "This transaction shouldn't be successful" );
        }
        catch ( TransactionFailureException e )
        {
            // Good
        }
    }

    private Node createNodeWithOneRelationshipToIt( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }
}
