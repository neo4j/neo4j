/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.recovery;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.EphemeralFileSystemRule.shutdownDb;

/**
 * Arbitrary recovery scenarios boiled down to as small tests as possible
 */
public class TestRecoveryScenarios
{
    @Test
    public void shouldRecoverTransactionWhereNodeIsDeletedInTheFuture() throws Exception
    {
        // GIVEN
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( label );
            node.setProperty( "key", "value" );
            tx.success();
        }
        rotateLog();
        try ( Transaction tx = db.beginTx() )
        {
            node.setProperty( "other-key", 1 );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            tx.success();
        }
        flushAll();

        // WHEN
        FileSystemAbstraction uncleanFs = fsRule.snapshot( shutdownDb( db ) );
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( uncleanFs ).newImpermanentDatabase();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            node = db.getNodeById( node.getId() );
            fail( "Should not exist" );
        }
        catch ( NotFoundException e )
        {
            assertEquals( "Node " + node.getId() + " not found", e.getMessage() );
        }
    }

    public final @Rule EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final Label label = label( "label" );
    private GraphDatabaseAPI db;

    @Before
    public void before()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
            .setFileSystem( fsRule.get() ).newImpermanentDatabase();
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    private void rotateLog()
    {
        db.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).rotateLogicalLogs();
    }

    private void flushAll()
    {
        db.getDependencyResolver().resolveDependency(
                XaDataSourceManager.class ).getNeoStoreDataSource().getNeoStore().flushAll();
    }
}
