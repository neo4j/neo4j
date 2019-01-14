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
package recovery;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;

/**
 * Arbitrary recovery scenarios boiled down to as small tests as possible
 */
@RunWith( Parameterized.class )
public class TestRecoveryScenarios
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final Label label = label( "label" );
    private GraphDatabaseAPI db;

    private final FlushStrategy flush;

    @SuppressWarnings( "deprecation" )
    @Before
    public void before()
    {
        db = (GraphDatabaseAPI) databaseFactory( fsRule.get() ).newImpermanentDatabase();
    }

    public TestRecoveryScenarios( FlushStrategy flush )
    {
        this.flush = flush;
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    @Test
    public void shouldRecoverTransactionWhereNodeIsDeletedInTheFuture() throws Exception
    {
        // GIVEN
        Node node = createNodeWithProperty( "key", "value", label );
        checkPoint();
        setProperty( node, "other-key", 1 );
        deleteNode( node );
        flush.flush( db );

        // WHEN
        crashAndRestart();

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
        checkPoint();
        addLabel( node, label );
        removeProperty( node, "key" );
        flush.flush( db );

        // WHEN
        crashAndRestart();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( "Updates not propagated correctly during recovery", Collections.<Node>emptyList(),
                    Iterators.asList( db.findNodes( label, "key", "value" ) ) );
            tx.success();
        }
    }

    @Test
    public void shouldRecoverTransactionWhereManyLabelsAreRemovedInTheFuture() throws Exception
    {
        // GIVEN
        createIndex( label, "key" );
        Label[] labels = new Label[16];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = label( "Label" + Integer.toHexString( i ) );
        }
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( labels );
            node.addLabel( label );
            tx.success();
        }
        checkPoint();
        setProperty( node, "key", "value" );
        removeLabels( node, labels );
        flush.flush( db );

        // WHEN
        crashAndRestart();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( node, db.findNode( label, "key", "value" ) );
            tx.success();
        }
    }

    @Test
    public void shouldRecoverCounts() throws Exception
    {
        // GIVEN
        Node node = createNode( label );
        checkPoint();
        deleteNode( node );

        // WHEN
        crashAndRestart();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( org.neo4j.internal.kernel.api.Transaction tx = db.getDependencyResolver().resolveDependency( Kernel.class ).beginTransaction(
                explicit, LoginContext.AUTH_DISABLED ) )
        {
            assertEquals( 0, tx.dataRead().countsForNode( -1 ) );
            final TokenHolder holder = db.getDependencyResolver().resolveDependency( TokenHolders.class ).labelTokens();
            int labelId = holder.getIdByName( label.name() );
            assertEquals( 0, tx.dataRead().countsForNode( labelId ) );
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

    private Node createNode( Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            tx.success();
            return node;
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

    @Parameterized.Parameters( name = "{0}" )
    public static List<Object[]> flushStrategy()
    {
        List<Object[]> parameters = new ArrayList<>(  );
        for ( FlushStrategy flushStrategy : FlushStrategy.values() )
        {
            parameters.add( flushStrategy.parameters );
        }
        return parameters;
    }

    @SuppressWarnings( "deprecation" )
    public enum FlushStrategy
    {
        FORCE_EVERYTHING
                {
                    @Override
                    void flush( GraphDatabaseAPI db )
                    {
                        IOLimiter limiter = IOLimiter.UNLIMITED;
                        db.getDependencyResolver().resolveDependency( StorageEngine.class ).flushAndForce( limiter );
                    }
                },
        FLUSH_PAGE_CACHE
                {
                    @Override
                    void flush( GraphDatabaseAPI db ) throws IOException
                    {
                        db.getDependencyResolver().resolveDependency( PageCache.class ).flushAndForce();
                    }
                };
        final Object[] parameters = new Object[]{this};

        abstract void flush( GraphDatabaseAPI db ) throws IOException;
    }

    private void checkPoint() throws IOException
    {
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
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

    private TestGraphDatabaseFactory databaseFactory( FileSystemAbstraction fs )
    {
        return new TestGraphDatabaseFactory().setFileSystem( fs );
    }

    @SuppressWarnings( "deprecation" )
    private void crashAndRestart() throws Exception
    {
        final GraphDatabaseService db1 = db;
        FileSystemAbstraction uncleanFs = fsRule.snapshot( db1::shutdown );
        db = (GraphDatabaseAPI) databaseFactory( uncleanFs ).newImpermanentDatabase();
    }
}
