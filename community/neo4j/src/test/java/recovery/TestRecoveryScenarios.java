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
package recovery;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.single;
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
        Node node = createNodeWithProperty( "key", "value", label );
        rotateLog();
        setProperty( node, "other-key", 1 );
        deleteNode( node );
        flushAll();

        // WHEN
        crashAndRestart( indexProvider );

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            node = db.getNodeById( node.getId() );
            tx.success();
            fail( "Should not exist" );
        }
        catch ( NotFoundException e )
        {
            assertEquals( "Node " + node.getId() + " not found", e.getMessage() );
        }
    }

    @Test
    public void shouldRecoverTransactionWherePropertyIsRemovedInTheFuture() throws Exception
    {
        // GIVEN
        createIndex( label, "key" );
        Node node = createNodeWithProperty( "key", "value" );
        rotateLog();
        addLabel( node, label );
        InMemoryIndexProvider outdatedIndexProvider = indexProvider.snapshot();
        removeProperty( node, "key" );
        flushAll();

        // WHEN
        crashAndRestart( outdatedIndexProvider );

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( "Updates not propagated correctly during recovery", Collections.<Node>emptyList(),
                          IteratorUtil.asList( db.findNodesByLabelAndProperty( label, "key", "value" ) ) );
            tx.success();
        }
    }

    @Test
    public void shouldRecoverTransactionWhereManyLabelsAreRemovedInTheFuture() throws Exception
    {
        // GIVEN
        createIndex( label, "key" );
        Label[] labels = new Label[16];
        for (int i = 0; i < labels.length; i++ )
        {
            labels[i] = DynamicLabel.label( "Label" + Integer.toHexString( i ) );
        }
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( labels );
            node.addLabel( label );
            tx.success();
        }
        rotateLog();
        InMemoryIndexProvider outdatedIndexProvider = indexProvider.snapshot();
        setProperty( node, "key", "value" );
        removeLabels( node, labels );
        flushAll();

        // WHEN
        crashAndRestart( outdatedIndexProvider );

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( node, single( db.findNodesByLabelAndProperty( label, "key", "value" ) ) );
            tx.success();
        }
    }

    private void removeLabels( Node node, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Label label : labels )
            {
                node.removeLabel( label );
            }
            tx.success();
        }
    }

    private void removeProperty( Node node, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            node.removeProperty( key );
            tx.success();
        }
    }

    private void addLabel( Node node, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            node.addLabel( label );
            tx.success();
        }
    }

    private Node createNodeWithProperty( String key, String value, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            node.setProperty( key, value );
            tx.success();
            return node;
        }
    }

    private void createIndex( Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( key ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }

    public final @Rule EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final Label label = label( "label" );
    @SuppressWarnings("deprecation") private GraphDatabaseAPI db;
    private final InMemoryIndexProvider indexProvider = new InMemoryIndexProvider( 100 );

    @SuppressWarnings("deprecation")
    @Before
    public void before()
    {
        db = (GraphDatabaseAPI) databaseFactory( fsRule.get(), indexProvider ).newImpermanentDatabase();
    }

    private TestGraphDatabaseFactory databaseFactory( FileSystemAbstraction fs, InMemoryIndexProvider indexProvider )
    {
        return new TestGraphDatabaseFactory()
            .setFileSystem( fs ).addKernelExtension( new InMemoryIndexProviderFactory( indexProvider ) );
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

    @SuppressWarnings("deprecation")
    private void flushAll()
    {
        db.getDependencyResolver().resolveDependency(
                XaDataSourceManager.class ).getNeoStoreDataSource().getNeoStore().flushAll();
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = db.beginTx() )
        {
            node.delete();
            tx.success();
        }
    }

    private void setProperty( Node node, String key, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            node.setProperty( key, value );
            tx.success();
        }
    }

    @SuppressWarnings("deprecation")
    private void crashAndRestart( InMemoryIndexProvider indexProvider )
    {
        FileSystemAbstraction uncleanFs = fsRule.snapshot( shutdownDb( db ) );
        db = (GraphDatabaseAPI) databaseFactory( uncleanFs, indexProvider ).newImpermanentDatabase();
    }
}
